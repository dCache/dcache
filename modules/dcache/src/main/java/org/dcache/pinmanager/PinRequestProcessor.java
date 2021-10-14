package org.dcache.pinmanager;


import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.pinmanager.model.Pin.State.PINNED;
import static org.dcache.pinmanager.model.Pin.State.PINNING;
import static org.dcache.pinmanager.model.Pin.State.UNPINNING;
import static org.springframework.transaction.annotation.Isolation.REPEATABLE_READ;

import diskCacheV111.poolManager.RequestContainerV5;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.FileNotOnlineCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Pool;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.PatternSyntaxException;
import javax.security.auth.Subject;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageReply;
import org.dcache.namespace.FileAttribute;
import org.dcache.pinmanager.model.Pin;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.PoolSelector;
import org.dcache.poolmanager.SelectedPool;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.annotation.Transactional;

/**
 * Processes pin requests.
 * <p>
 * A pin request goes through several steps to pin a file on a pool:
 * <p>
 * - Create DB entry in state PINNING - Optionally read the name space entry - Select a read pool
 * (which may involve staging) - Update DB entry with the pool name - Create sticky flag on pool -
 * Update DB entry to state PINNED
 * <p>
 * If during any step the entry is no longer in PINNING then the operation is aborted.
 * <p>
 * If a DB error occurs it is considered fatal and the pinning operation is not completed. The DB
 * entry will stay in PINNING until either explicitly unpinned or it expires.
 * <p>
 * Database operations are blocking. Communication with PoolManager and pools is asynchronous.
 */
public class PinRequestProcessor
      implements CellMessageReceiver {

    private static final Logger _log =
          LoggerFactory.getLogger(PinRequestProcessor.class);

    /**
     * The delay we use after a pin request failed and before retrying the request.
     */
    private static final long RETRY_DELAY = SECONDS.toMillis(30);

    /**
     * The delay we use after transient failures that should be retried immediately. The small delay
     * prevents tight retry loops.
     */
    private static final long SMALL_DELAY = MILLISECONDS.toMillis(10);

    /**
     * Safety margin added to the lifetime of the sticky bit to account for clock drift.
     */
    private static final long CLOCK_DRIFT_MARGIN = MINUTES.toMillis(30);

    private ScheduledExecutorService _scheduledExecutor;
    private Executor _executor;
    private PinDao _dao;
    private CellStub _poolStub;
    private CellStub _pnfsStub;
    private PoolManagerStub _poolManagerStub;
    private CheckStagePermission _checkStagePermission;
    private long _maxLifetime;
    private TimeUnit _maxLifetimeUnit;

    private PoolMonitor _poolMonitor;

    @Required
    public void setScheduledExecutor(ScheduledExecutorService executor) {
        _scheduledExecutor = executor;
    }

    @Required
    public void setExecutor(Executor executor) {
        _executor = executor;
    }

    @Required
    public void setDao(PinDao dao) {
        _dao = dao;
    }

    @Required
    public void setPoolStub(CellStub stub) {
        _poolStub = stub;
    }

    @Required
    public void setPnfsStub(CellStub stub) {
        _pnfsStub = stub;
    }

    @Required
    public void setPoolManagerStub(PoolManagerStub stub) {
        _poolManagerStub = stub;
    }

    @Required
    public void setStagePermission(CheckStagePermission checker) {
        _checkStagePermission = checker;
    }

    @Required
    public void setMaxLifetime(long maxLifetime) {
        _maxLifetime = maxLifetime;
    }

    @Required
    public void setPoolMonitor(PoolMonitor poolMonitor) {
        _poolMonitor = poolMonitor;
    }

    public long getMaxLifetime() {
        return _maxLifetime;
    }

    @Required
    public void setMaxLifetimeUnit(TimeUnit unit) {
        _maxLifetimeUnit = unit;
    }

    public TimeUnit getMaxLifetimeUnit() {
        return _maxLifetimeUnit;
    }

    private void enforceLifetimeLimit(PinManagerPinMessage message) {
        if (_maxLifetime > -1) {
            long millis = _maxLifetimeUnit.toMillis(_maxLifetime);
            long requestedLifetime = message.getLifetime();
            if (requestedLifetime == -1) {
                message.setLifetime(millis);
            } else {
                message.setLifetime(Math.min(millis, requestedLifetime));
            }
        }
    }

    public MessageReply<PinManagerPinMessage>
    messageArrived(PinManagerPinMessage message)
          throws CacheException {
        MessageReply<PinManagerPinMessage> reply =
              new MessageReply<>();

        enforceLifetimeLimit(message);

        PinTask task = createTask(message, reply);
        if (task != null) {
            if (!task.getFileAttributes()
                  .isDefined(PoolMgrSelectReadPoolMsg.getRequiredAttributes())) {
                rereadNameSpaceEntry(task);
            } else {
                selectReadPool(task);
            }
            if (message.isReplyWhenStarted()) {
                reply.reply(message);
            }
        }

        return reply;
    }

    protected EnumSet<RequestContainerV5.RequestState>
    checkStaging(PinTask task) {
        if (task.isStagingDenied()) {
            return RequestContainerV5.allStatesExceptStage;
        }

        try {
            Subject subject = task.getSubject();
            return _checkStagePermission.canPerformStaging(subject,
                  task.getFileAttributes(),
                  task.getProtocolInfo()) ?
                  RequestContainerV5.allStates :
                  RequestContainerV5.allStatesExceptStage;
        } catch (PatternSyntaxException | IOException ex) {
            _log.error("Failed to check stage permission: {}", ex.toString());
        }
        return RequestContainerV5.allStatesExceptStage;
    }

    private void retry(final PinTask task, long delay) {
        if (!task.isValidIn(delay)) {
            fail(task, CacheException.TIMEOUT, "Pin request TTL exceeded");
        } else {
            _scheduledExecutor.schedule(() -> {
                try {
                    rereadNameSpaceEntry(task);
                } catch (CacheException e) {
                    fail(task, e.getRc(), e.getMessage());
                } catch (RuntimeException e) {
                    fail(task, CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.toString());
                }
            }, delay, MILLISECONDS);
        }
    }

    private void fail(PinTask task, int rc, String error) {
        try {
            task.fail(rc, error);
            clearPin(task);
        } catch (RuntimeException e) {
            _log.error(e.toString());
        }
    }

    private void rereadNameSpaceEntry(final PinTask task)
          throws CacheException {
        /* Ensure that task is still valid and stays valid for the
         * duration of the name space lookup.
         */
        refreshTimeout(task, getExpirationTimeForNameSpaceLookup());

        /* We allow the set of provided attributes to be incomplete
         * and thus add attributes required by pool manager.
         */
        Set<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);
        attributes.addAll(task.getFileAttributes().getDefinedAttributes());
        attributes.addAll(PoolMgrSelectReadPoolMsg.getRequiredAttributes());

        CellStub.addCallback(
              _pnfsStub.send(new PnfsGetFileAttributes(task.getPnfsId(), attributes)),
              new AbstractMessageCallback<PnfsGetFileAttributes>() {
                  @Override
                  public void success(PnfsGetFileAttributes msg) {
                      try {
                          task.setFileAttributes(msg.getFileAttributes());

                          /* Ensure that task is still valid
                           * and stays valid for the duration
                           * of the pool selection.
                           */
                          refreshTimeout(task, getExpirationTimeForPoolSelection());
                          selectReadPool(task);
                      } catch (CacheException e) {
                          fail(task, e.getRc(), e.getMessage());
                      } catch (RuntimeException e) {
                          fail(task, CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.toString());
                      }
                  }

                  @Override
                  public void failure(int rc, Object error) {
                      fail(task, rc, error.toString());
                  }

                  @Override
                  public void noroute(CellPath path) {
                      /* PnfsManager is unreachable. We
                       * expect this to be a transient
                       * problem and retry in a moment.
                       */
                      retry(task, RETRY_DELAY);
                  }

                  @Override
                  public void timeout(String error) {
                      /* PnfsManager did not respond. We
                       * expect this to be a transient
                       * problem and retry in a moment.
                       */
                      retry(task, SMALL_DELAY);
                  }
              }, _executor);
    }

    private void selectReadPool(final PinTask task)
          throws CacheException {
        try {
            PoolSelector poolSelector =
                  _poolMonitor.getPoolSelector(task.getFileAttributes(),
                        task.getProtocolInfo(),
                        null,
                        Collections.EMPTY_SET);

            SelectedPool pool = poolSelector.selectPinPool();
            setPool(task, pool.name());
            setStickyFlag(task, pool.name(), pool.address());
        } catch (FileNotOnlineCacheException e) {
            askPoolManager(task);
        }
    }

    private void askPoolManager(final PinTask task) {
        PoolMgrSelectReadPoolMsg msg =
              new PoolMgrSelectReadPoolMsg(task.getFileAttributes(),
                    task.getProtocolInfo(),
                    task.getReadPoolSelectionContext(),
                    checkStaging(task));
        msg.setSubject(task.getSubject());
        CellStub.addCallback(_poolManagerStub.sendAsync(msg),
              new AbstractMessageCallback<PoolMgrSelectReadPoolMsg>() {
                  @Override
                  public void success(PoolMgrSelectReadPoolMsg msg) {
                      try {
                          /* Pool manager expects us
                           * to keep some state
                           * between retries.
                           */
                          task.setReadPoolSelectionContext(msg.getContext());

                          /* Store the pool name in
                           * the DB so we know what to
                           * clean up if something
                           * fails.
                           */
                          Pool pool = msg.getPool();
                          task.getFileAttributes().getLocations().add(pool.getName());
                          setPool(task, pool.getName());

                          setStickyFlag(task, pool.getName(), pool.getAddress());
                      } catch (CacheException e) {
                          fail(task, e.getRc(), e.getMessage());
                      } catch (RuntimeException e) {
                          fail(task, CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.toString());
                      }
                  }

                  @Override
                  public void failure(int rc, Object error) {
                      /* Pool manager expects us to
                       * keep some state between
                       * retries.
                       */
                      task.setReadPoolSelectionContext(getReply().getContext());
                      switch (rc) {
                          case CacheException.OUT_OF_DATE:
                              /* Pool manager asked for a
                               * refresh of the request.
                               * Retry right away.
                               */
                              retry(task, 0);
                              break;
                          case CacheException.FILE_NOT_IN_REPOSITORY:
                          case CacheException.PERMISSION_DENIED:
                              fail(task, rc, error.toString());
                              break;
                          default:
                              /* Ideally we would delegate the retry to the door,
                               * but for the time being the retry is dealed with
                               * by pin manager.
                               */
                              retry(task, RETRY_DELAY);
                              break;
                      }
                  }

                  @Override
                  public void noroute(CellPath path) {
                      /* Pool manager is
                       * unreachable. We expect this
                       * to be transient and retry in
                       * a moment.
                       */
                      retry(task, RETRY_DELAY);
                  }

                  @Override
                  public void timeout(String message) {
                      /* Pool manager did not
                       * respond. We expect this to be
                       * transient and retry in a
                       * moment.
                       */
                      retry(task, SMALL_DELAY);
                  }
              }, _executor);
    }

    private void setStickyFlag(final PinTask task, final String poolName,
          CellAddressCore poolAddress) {
        /* The pin lifetime should be from the moment the file is
         * actually pinned. Due to staging and pool to pool transfers
         * this may be much later than when the pin was requested.
         */
        Date pinExpiration = task.freezeExpirationTime();

        /* To allow for some drift in clocks we add a safety margin to
         * the lifetime of the sticky bit.
         */
        long poolExpiration =
              (pinExpiration == null) ? -1 : pinExpiration.getTime() + CLOCK_DRIFT_MARGIN;

        PoolSetStickyMessage msg =
              new PoolSetStickyMessage(poolName,
                    task.getPnfsId(),
                    true,
                    task.getSticky(),
                    poolExpiration);
        CellStub.addCallback(_poolStub.send(new CellPath(poolAddress), msg),
              new AbstractMessageCallback<PoolSetStickyMessage>() {
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
                              /* Pool manager had outdated
                               * information about the pool. Give
                               * it a chance to be updated and
                               * then retry.
                               */
                              retry(task, RETRY_DELAY);
                              break;
                          case CacheException.FILE_NOT_IN_REPOSITORY:
                              /* Pnfs manager had stale location
                               * information. The pool clears
                               * this information as a result of
                               * this error, so we retry in a
                               * moment.
                               */
                              retry(task, SMALL_DELAY);
                              break;
                          default:
                              fail(task, rc, error.toString());
                              break;
                      }
                  }

                  @Override
                  public void noroute(CellPath path) {
                      /* The pool must have gone down. Give
                       * pool manager a moment to notice this
                       * and then retry.
                       */
                      retry(task, RETRY_DELAY);
                  }

                  @Override
                  public void timeout(String error) {
                      /* No response from pool. Typically this is
                       * because the pool is overloaded.
                       */
                      fail(task, CacheException.TIMEOUT, error);
                  }
              }, _executor);
    }

    private Date getExpirationTimeForNameSpaceLookup() {
        long now = System.currentTimeMillis();
        long timeout = _pnfsStub.getTimeoutInMillis();
        return new Date(now + 2 * (timeout + RETRY_DELAY));
    }

    private Date getExpirationTimeForPoolSelection() {
        long now = System.currentTimeMillis();
        long timeout = _poolManagerStub.getPoolManagerTimeoutInMillis();
        return new Date(now + 2 * (timeout + RETRY_DELAY));
    }

    private Date getExpirationTimeForSettingFlag() {
        long now = System.currentTimeMillis();
        long timeout = _poolStub.getTimeoutInMillis();
        return new Date(now + 2 * timeout);
    }

    @Transactional(isolation = REPEATABLE_READ)
    protected PinTask createTask(PinManagerPinMessage message,
          MessageReply<PinManagerPinMessage> reply) {
        PnfsId pnfsId = message.getFileAttributes().getPnfsId();

        if (message.getRequestId() != null) {
            Pin pin = _dao.get(_dao.where().pnfsId(pnfsId).requestId(message.getRequestId()));
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

                _dao.update(pin, _dao.set().state(UNPINNING).requestId(null));
            }
        }

        Pin pin = _dao.create(_dao.set()
              .subject(message.getSubject())
              .state(PINNING)
              .pnfsId(pnfsId)
              .requestId(message.getRequestId())
              .sticky("PinManager-" + UUID.randomUUID().toString())
              .expirationTime(getExpirationTimeForPoolSelection()));

        return new PinTask(message, reply, pin);
    }

    private void updateTask(PinTask task, PinDao.PinUpdate update) throws CacheException {
        Pin pin = _dao.update(
              _dao.where().id(task.getPinId()).sticky(task.getSticky()).state(PINNING), update);
        if (pin == null) {
            throw new CacheException("Operation was aborted");
        }
        task.setPin(pin);
    }


    @Transactional(isolation = REPEATABLE_READ)
    protected void refreshTimeout(PinTask task, Date date)
          throws CacheException {
        updateTask(task, _dao.set().expirationTime(date));
    }

    @Transactional(isolation = REPEATABLE_READ)
    protected void setPool(PinTask task, String pool)
          throws CacheException {
        updateTask(task, _dao.set().expirationTime(getExpirationTimeForSettingFlag()).pool(pool));
    }

    @Transactional(isolation = REPEATABLE_READ)
    protected void setToPinned(PinTask task)
          throws CacheException {
        updateTask(task, _dao.set().expirationTime(task.getExpirationTime()).state(PINNED));
    }

    @Transactional
    protected void clearPin(PinTask task) {
        if (task.getPool() != null) {
            /* If the pin record expired or the pin was explicitly
             * unpinned, then the unpin processor may already have
             * submitted a request to the pool to clear the sticky
             * flag. Although out of order delivery of messages is
             * unlikely, if it would happen then we have a race
             * between the set sticky and clear sticky messages. To
             * cover this case we delete the old record and create a
             * fresh one in UNPINNING.
             */
            _dao.delete(task.getPin());
            _dao.create(_dao.set()
                  .subject(task.getSubject())
                  .pnfsId(task.getPnfsId())
                  .state(UNPINNING));
        } else {
            /* We didn't create a sticky flag yet, so there is no
             * reason to keep the record. It will expire by itself,
             * but we delete the record now to avoid that we get
             * tickets from admins wondering why they have records
             * staying in PINNING.
             */
            _dao.delete(task.getPin());
        }
    }
}
