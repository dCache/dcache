package org.dcache.pool.migration;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.net.UnknownHostException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.pool.p2p.P2PClient;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.IllegalTransitionException;
import static org.dcache.pool.repository.EntryState.*;

import dmg.util.Args;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.apache.log4j.Logger;

/**
 * Server component of migration module.
 *
 * This class is essentially a migration module specific frontend to
 * the pool to pool transfer component.
 *
 * The following messages are handled:
 *
 *   - PoolMigrationUpdateReplicaMessage
 *   - PoolMigrationCopyReplicaMessage
 *   - PoolMigrationPingMessage
 *   - PoolMigrationCancelMessage
 */
public class MigrationModuleServer
    extends AbstractCellComponent
    implements CellMessageReceiver
{
    private final static Logger _log =
        Logger.getLogger(MigrationModuleServer.class);

    private Map<Request,Integer> _requests = new HashMap();
    private P2PClient _p2p;
    private Repository _repository;

    public void setPPClient(P2PClient p2p)
    {
        _p2p = p2p;
    }

    public void setRepository(Repository repository)
    {
        _repository = repository;
    }

    public Message messageArrived(PoolMigrationUpdateReplicaMessage message)
        throws CacheException
    {
        PnfsId pnfsId = message.getPnfsId();
        EntryState targetState = message.getState();
        CacheEntry entry = _repository.getEntry(pnfsId);
        EntryState state = entry.getState();
        long ttl = message.getTimeToLive();

        /* Drop message if TTL is exceeded. This is done to avoid
         * modifying the entry after the requestor has timed out.
         */
        if (ttl < System.currentTimeMillis()) {
            return null;
        }

        if (targetState != PRECIOUS && targetState != CACHED) {
            throw new IllegalArgumentException("State must be either CACHED or PRECIOUS");
        }

        try {
            switch (state) {
            case CACHED:
                if (targetState == PRECIOUS) {
                    _repository.setState(pnfsId, PRECIOUS);
                }
                // fall through
            case PRECIOUS:
                for (StickyRecord record: message.getStickyRecords()) {
                    _repository.setSticky(pnfsId,
                                          record.owner(),
                                          record.expire(),
                                          false);
                }
                break;
            default:
                throw new CacheException(CacheException.DEFAULT_ERROR_CODE,
                                         "Cannot update file in state " + state);
            }
        } catch (IllegalTransitionException e) {
            throw new CacheException(CacheException.DEFAULT_ERROR_CODE,
                                     "Cannot update file in state " + e.getSourceState());
        }

        message.setSucceeded();
        return message;
    }

    synchronized public Message
        messageArrived(CellMessage envelope, PoolMigrationCopyReplicaMessage message)
        throws UnknownHostException
    {
        final long taskId = message.getTaskId();
        final PnfsId pnfsId = message.getPnfsId();
        final String pool = message.getPool();
        final StorageInfo storageInfo = message.getStorageInfo();
        final List<StickyRecord> stickyRecords = message.getStickyRecords();
        final EntryState state = message.getState();
        final CellPath requestor = (CellPath)envelope.getSourcePath().clone();
        final Request request = new Request(pool, taskId);

        CacheFileAvailable callback =
            new CacheFileAvailable()
            {
                public void cacheFileAvailable(String file, Throwable e)
                {
                    synchronized (MigrationModuleServer.this) {
                        _requests.remove(request);
                    }

                    PoolMigrationCopyFinishedMessage message =
                        new PoolMigrationCopyFinishedMessage(pool, pnfsId, taskId);
                    if (e != null) {
                        if (e instanceof CacheException) {
                            CacheException ce = (CacheException) e;
                            message.setFailed(ce.getRc(), ce.getMessage());
                        } else {
                            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                              e);
                        }
                    }

                    try {
                        requestor.revert();
                        sendMessage(new CellMessage(requestor, message));
                    } catch (NoRouteToCellException f) {
                        // We cannot tell the requestor that the
                        // transfer has finished. Not much we can do
                        // about that. The requestor will eventually
                        // notice that nothing happens.
                        _log.error("Transfer completed, but failed to send acknowledgment: " +
                                   f.toString());
                    }
                }
            };

        int companionId = _p2p.newCompanion(pnfsId, pool, storageInfo,
                                            state, stickyRecords, callback);

        _requests.put(request, companionId);

        message.setSucceeded();
        return message;
    }

    synchronized public Message messageArrived(PoolMigrationPingMessage message)
    {
        long taskId = message.getTaskId();
        String pool = message.getPool();
        Request request = new Request(pool, taskId);

        if (_requests.containsKey(request)) {
            message.setSucceeded();
        } else {
            message.setFailed(CacheException.INVALID_ARGS, "No such request");
        }

        return message;
    }

    synchronized public Message messageArrived(PoolMigrationCancelMessage message)
        throws CacheException
    {
        long taskId = message.getTaskId();
        String pool = message.getPool();
        Request request = new Request(pool, taskId);
        Integer companionId = _requests.get(request);
        if (companionId == null) {
            throw new CacheException(CacheException.INVALID_ARGS,
                                     "No such request");
        }

        _p2p.cancel(companionId);

        message.setSucceeded();
        return message;
    }


    private static class Request
    {
        String pool;
        long taskId;

        public Request(String pool, long taskId)
        {
            this.pool = pool;
            this.taskId = taskId;
        }

        public int hashCode()
        {
            return pool.hashCode() ^ (int) taskId;
        }

        public boolean equals(Object o)
        {
            if (! (o instanceof Request)) {
                return false;
            }
            Request r = (Request) o;
            return pool.equals(r.pool) && taskId == r.taskId;
        }
    }
}