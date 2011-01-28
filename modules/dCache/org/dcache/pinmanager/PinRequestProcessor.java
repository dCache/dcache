package org.dcache.pinmanager;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import java.util.EnumSet;
import java.util.regex.PatternSyntaxException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import javax.security.auth.Subject;
import static java.util.concurrent.TimeUnit.*;

import org.dcache.cells.CellStub;
import org.dcache.cells.MessageReply;
import org.dcache.cells.MessageCallback;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.auth.Subjects;
import org.dcache.pinmanager.model.Pin;
import static org.dcache.pinmanager.model.Pin.State.*;

import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.PnfsId;
import diskCacheV111.poolManager.RequestContainerV5;
import org.dcache.vehicles.FileAttributes;
import org.dcache.pinmanager.PinManagerPinMessage;

import dmg.cells.nucleus.CellPath;

import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.annotation.Transactional;
import static org.springframework.transaction.annotation.Isolation.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes pin requests.
 *
 * A pin request goes through several steps to pin a file on a pool:
 *
 * - Create DB entry in state PINNING
 * - Select a read pool (which may involve staging)
 * - Update DB entry with the pool name
 * - Create sticky flag on pool
 * - Update DB entry to state PINNED
 *
 * If during any step the entry is no longer in PINNING then the
 * operation is aborted.
 *
 * If a DB error occurs it is considered fatal and the pinning
 * operation is not completed. The DB entry will stay in PINNING until
 * either explicitly unpinned or it expires the next time the
 * PinManager is started.
 *
 * Database operations are blocking. Communication with PoolManager
 * and pools is asynchronous.
 */
public class PinRequestProcessor
    implements CellMessageReceiver
{
    private final static Logger _log =
        LoggerFactory.getLogger(PinRequestProcessor.class);

    private final static long RETRY_DELAY = SECONDS.toMillis(30);
    private final static long SMALL_DELAY = MILLISECONDS.toMillis(10);
    private final static long POOL_LIFETIME_MARGIN = MINUTES.toMillis(30);

    private ScheduledExecutorService _executor;
    private PinDao _dao;
    private CellStub _poolStub;
    private CellStub _poolManagerStub;
    private CheckStagePermission _checkStagePermission;
    private long _maxLifetime;

    @Required
    public void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
    }

    @Required
    public void setDao(PinDao dao)
    {
        _dao = dao;
    }

    @Required
    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    @Required
    public void setPoolManagerStub(CellStub stub)
    {
        _poolManagerStub = stub;
    }

    @Required
    public void setStagePermission(CheckStagePermission checker)
    {
        _checkStagePermission = checker;
    }

    @Required
    public void setMaxLifetime(long maxLifetime)
    {
        _maxLifetime = maxLifetime;
    }

    public long getMaxLifetime()
    {
        return _maxLifetime;
    }

    public MessageReply<PinManagerPinMessage>
        messageArrived(PinManagerPinMessage message)
        throws CacheException, InterruptedException, ExecutionException
    {
        MessageReply<PinManagerPinMessage> reply =
            new MessageReply<PinManagerPinMessage>();

        if (_maxLifetime > -1) {
            message.setLifetime(Math.min(_maxLifetime, message.getLifetime()));
        }

        PinTask task = createTask(message, reply);
        if (task != null) {
            selectReadPool(task);
        }

        return reply;
    }

    protected EnumSet<RequestContainerV5.RequestState> checkStaging(PinTask task)
    {
        try {
            Subject subject = task.getSubject();
            StorageInfo info = task.getStorageInfo();
            return _checkStagePermission.canPerformStaging(subject, info) ?
                RequestContainerV5.allStates :
                RequestContainerV5.allStatesExceptStage;
        } catch (PatternSyntaxException ex) {
            _log.error("Failed to check stage permission: " + ex);
        } catch (IOException ex) {
            _log.error("Failed to check stage permission: " + ex);
        }
        return RequestContainerV5.allStatesExceptStage;
    }

    private void retry(final PinTask task, long delay)
    {
        if (!task.isValidIn(delay)) {
            fail(task, CacheException.TIMEOUT, "Request timed out");
        } else {
            _executor.schedule(new Runnable() {
                    public void run() {
                        try {
                            refreshTimeout(task);
                            selectReadPool(task);
                        } catch (CacheException e) {
                            fail(task, e.getRc(), e.getMessage());
                        }
                    }
                }, delay, TimeUnit.MILLISECONDS);
        }
    }

    private void fail(PinTask task, int rc, String error)
    {
        try {
            task.fail(rc, error);
            clearPin(task);
        } catch (RuntimeException e) {
            _log.error(e.toString());
        }
    }

    private void selectReadPool(final PinTask task)
    {
        PoolMgrSelectReadPoolMsg msg =
            new PoolMgrSelectReadPoolMsg(task.getPnfsId(),
                                         task.getStorageInfo(),
                                         task.getProtocolInfo(),
                                         0,
                                         checkStaging(task));
        _poolManagerStub.send(msg,
                              PoolMgrSelectReadPoolMsg.class,
                              new MessageCallback<PoolMgrSelectReadPoolMsg>()
                              {
                                  @Override
                                  public void success(PoolMgrSelectReadPoolMsg msg)
                                  {
                                      try {
                                          String pool = msg.getPoolName();
                                          setPool(task, pool);
                                          setStickyFlag(task, pool);
                                      } catch (CacheException e) {
                                          fail(task, e.getRc(), e.getMessage());
                                      } catch (RuntimeException e) {
                                          fail(task, CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.toString());
                                      }
                                  }

                                  @Override
                                  public void failure(int rc, Object error)
                                  {
                                      fail(task, rc, error.toString());
                                  }

                                  @Override
                                  public void noroute()
                                  {
                                      retry(task, RETRY_DELAY);
                                  }

                                  @Override
                                  public void timeout()
                                  {
                                      retry(task, SMALL_DELAY);
                                  }
                              });
    }

    private void setStickyFlag(final PinTask task, String pool)
    {
        Date pinExpiration = task.freezeExpirationTime();
        long poolExpiration =
            (pinExpiration == null) ? -1 : pinExpiration.getTime() + POOL_LIFETIME_MARGIN;

        PoolSetStickyMessage msg =
            new PoolSetStickyMessage(pool,
                                     task.getPnfsId(),
                                     true,
                                     task.getSticky(),
                                     poolExpiration);
        _poolStub.send(new CellPath(pool), msg,
                       PoolSetStickyMessage.class,
                       new MessageCallback<PoolSetStickyMessage>() {
                           @Override
                           public void success(PoolSetStickyMessage msg) {
                               try {
                                   setToPinned(task);
                                   task.success();
                               } catch (CacheException e) {
                                   fail(task, e.getRc(), e.getMessage());
                               } catch (RuntimeException e) {
                                   fail(task, CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.toString());
                               }
                           }

                           @Override
                           public void failure(int rc, Object error) {
                               switch (rc) {
                               case CacheException.POOL_DISABLED:
                                   retry(task, RETRY_DELAY);
                                   break;
                               case CacheException.FILE_NOT_IN_REPOSITORY:
                                   retry(task, SMALL_DELAY);
                                   break;
                               default:
                                   fail(task, rc, error.toString());
                                   break;
                               }
                           }

                           @Override
                           public void noroute() {
                               retry(task, RETRY_DELAY);
                           }

                           @Override
                           public void timeout() {
                               fail(task, CacheException.TIMEOUT, "Request timed out");
                           }
                       });
    }

    private Date getExpirationTimeForPoolSelection()
    {
        long now = System.currentTimeMillis();
        long timeout = _poolManagerStub.getTimeout();
        return new Date(now + 2 * (timeout + RETRY_DELAY));
    }

    private Date getExpirationTimeForSettingFlag()
    {
        long now = System.currentTimeMillis();
        long timeout = _poolStub.getTimeout();
        return new Date(now + 2 * timeout);
    }

    @Transactional
    protected PinTask createTask(PinManagerPinMessage message,
                                 MessageReply<PinManagerPinMessage> reply)
    {
        PnfsId pnfsId = message.getFileAttributes().getPnfsId();

        if (message.getRequestId() != null) {
            Pin pin = _dao.getPin(pnfsId, message.getRequestId());
            if (pin != null) {
                /* In this case the request is a resubmission. If the
                 * previous pin completed then use it. Otherwise abort the
                 * previous pin and create a new one.
                 */
                if (pin.getState() == PINNED) {
                    message.setPin(pin);
                    reply.reply(message);
                    return null;
                }

                pin.setState(UNPINNING);
                pin.setRequestId(null);
                _dao.storePin(pin);
            }
        }

        Pin pin = new Pin(message.getSubject(), pnfsId);
        pin.setRequestId(message.getRequestId());
        pin.setSticky("PinManager-" + UUID.randomUUID().toString());
        pin.setExpirationTime(getExpirationTimeForPoolSelection());

        return new PinTask(message, reply, _dao.storePin(pin));
    }

    protected Pin load(PinTask task)
        throws CacheException
    {
        Pin pin = _dao.getPin(task.getPinId(), task.getSticky(), PINNING);
        if (pin == null) {
            throw new CacheException("Operation was aborted");
        }
        return pin;
    }

    @Transactional(isolation=REPEATABLE_READ)
    protected void refreshTimeout(PinTask task)
        throws CacheException
    {
        Pin pin = load(task);
        pin.setExpirationTime(getExpirationTimeForPoolSelection());
        task.setPin(_dao.storePin(pin));
    }

    @Transactional(isolation=REPEATABLE_READ)
    protected void setPool(PinTask task, String pool)
        throws CacheException
    {
        Pin pin = load(task);
        pin.setExpirationTime(getExpirationTimeForSettingFlag());
        pin.setPool(pool);
        task.setPin(_dao.storePin(pin));
    }

    @Transactional(isolation=REPEATABLE_READ)
    protected void setToPinned(PinTask task)
        throws CacheException
    {
        Pin pin = load(task);
        pin.setExpirationTime(task.getExpirationTime());
        pin.setState(PINNED);
        task.setPin(_dao.storePin(pin));
    }

    @Transactional
    protected void clearPin(PinTask task)
    {
        /* If there is no pool, then don't bother updating the record
         * - it will expire by itself. If there is a pool then we may
         * have a sticky flag set and an expired record. To cover this
         * case we delete the original record and create a new record
         * in UNPINNING.
         */
        if (task.getPool() != null) {
            _dao.deletePin(task.getPin());
            Pin pin = new Pin(task.getSubject(), task.getPnfsId());
            pin.setState(UNPINNING);
            _dao.storePin(pin);
        }
    }
}