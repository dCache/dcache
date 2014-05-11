package org.dcache.pool.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.LockedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellMessageReceiver;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.p2p.P2PClient;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.Repository.OpenFlags;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.pool.repository.EntryState.CACHED;
import static org.dcache.pool.repository.EntryState.PRECIOUS;

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
        LoggerFactory.getLogger(MigrationModuleServer.class);

    private Map<UUID, Request> _requests = new ConcurrentHashMap<>();
    private P2PClient _p2p;
    private Repository _repository;
    private ExecutorService _executor;
    private ChecksumModule _checksumModule;
    private MigrationModule _migration;
    private PoolV2Mode _poolMode;

    public void setExecutor(ExecutorService executor)
    {
        _executor = executor;
    }

    public void setPPClient(P2PClient p2p)
    {
        _p2p = p2p;
    }

    public void setRepository(Repository repository)
    {
        _repository = repository;
    }

    public synchronized void setChecksumModule(ChecksumModule csm)
    {
        _checksumModule = csm;
    }

    public void setMigrationModule(MigrationModule migration)
    {
        _migration = migration;
    }

    public void setPoolMode(PoolV2Mode mode)
    {
        _poolMode = mode;
    }

    public Message
        messageArrived(CellMessage envelope, PoolMigrationCopyReplicaMessage message)
        throws CacheException, IOException, InterruptedException
    {
        if (message.isReply()) {
            return null;
        }

        if (_poolMode.isDisabled(PoolV2Mode.DISABLED_P2P_CLIENT)) {
            throw new CacheException(CacheException.POOL_DISABLED,
                                     "Pool is disabled");
        }

        /* This check prevents updates that are indirectly triggered
         * by a local migration task: In particular the case in which
         * two pools each try to move the same files to each other
         * would otherwise have a race condition that would cause
         * files to be lost. This check prevents that any local
         * migration task is active on this file at this time and
         * hence the update request cannot be a result of a local
         * migration task.
         */
        if (_migration.isActive(message.getPnfsId())) {
            throw new LockedCacheException("Target file is busy");
        }

        Request request =
            new Request((CellPath)envelope.getSourcePath().clone(), message);
        _requests.put(request.getUUID(), request);
        request.start();

        message.setSucceeded();
        return message;
    }

    public Message messageArrived(PoolMigrationPingMessage message)
    {
        if (message.isReply()) {
            return null;
        }

        UUID uuid = message.getUUID();
        if (_requests.containsKey(uuid)) {
            message.setSucceeded();
        } else {
            message.setFailed(CacheException.INVALID_ARGS, "No such request");
        }

        return message;
    }

    public Message messageArrived(PoolMigrationCancelMessage message)
        throws CacheException
    {
        if (message.isReply()) {
            return null;
        }

        UUID uuid = message.getUUID();
        Request request = _requests.get(uuid);
        if (request == null || !request.cancel()) {
            throw new CacheException(CacheException.INVALID_ARGS,
                                     "No such request");
        }

        message.setSucceeded();
        return message;
    }

    private class Request implements CacheFileAvailable, Runnable
    {
        private final CellPath _requestor;
        private final UUID _uuid;
        private final PnfsId _pnfsId;
        private final FileAttributes _fileAttributes;
        private final List<StickyRecord> _stickyRecords;
        private final EntryState _targetState;
        private final String _pool;
        private final boolean _computeChecksumOnUpdate;
        private final boolean _forceSourceMode;
        private Integer _companion;
        private Future<?> _updateTask;

        public Request(CellPath requestor, PoolMigrationCopyReplicaMessage message)
        {
            _requestor = requestor;
            _pnfsId = message.getPnfsId();
            _fileAttributes = message.getFileAttributes();
            _stickyRecords = message.getStickyRecords();
            _targetState = message.getState();
            _pool = message.getPool();
            _computeChecksumOnUpdate = message.getComputeChecksumOnUpdate();
            _forceSourceMode = message.isForceSourceMode();

            if (_targetState != PRECIOUS && _targetState != CACHED) {
                throw new IllegalArgumentException("State must be either CACHED or PRECIOUS");
            }

            /* Old pools don't provide a UUID so we create one
             * ourselves. Will eventually be removed.
             */
            _uuid =
                (message.getUUID() != null) ? message.getUUID() : UUID.randomUUID();
        }

        public synchronized UUID getUUID()
        {
            return _uuid;
        }

        public synchronized String getPool()
        {
            return _pool;
        }

        public synchronized void start()
            throws IOException, CacheException, InterruptedException
        {
            EntryState state = _repository.getState(_pnfsId);
            if (state == EntryState.NEW) {
                _companion = _p2p.newCompanion(_pool, _fileAttributes,
                                               _targetState, _stickyRecords,
                                               this, _forceSourceMode);
            } else {
                _updateTask = _executor.submit(this);
            }
        }

        public synchronized boolean cancel()
        {
            if (_companion != null) {
                return _p2p.cancel(_companion);
            } else if (_updateTask != null && _updateTask.cancel(true)) {
                if (_requests.remove(_uuid) != null) {
                    finished(new CacheException("Task was cancelled"));
                }
                return true;
            }
            return false;
        }

        protected synchronized void finished(Throwable e)
        {
            PoolMigrationCopyFinishedMessage message =
                new PoolMigrationCopyFinishedMessage(_uuid, _pool, _pnfsId);
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
                sendMessage(new CellMessage(_requestor.revert(), message));
            } catch (NoRouteToCellException f) {
                // We cannot tell the requestor that the
                // transfer has finished. Not much we can do
                // about that. The requestor will eventually
                // notice that nothing happens.
                _log.error("Transfer completed, but failed to send acknowledgment: " +
                           f.toString());
            }
        }

        @Override
        public synchronized void cacheFileAvailable(PnfsId pnfsId, Throwable e)
        {
            _requests.remove(_uuid);
            finished(e);
        }

        /**
         * Executed to update an existing replica
         */
        @Override
        public void run()
        {
            try {
                if (_computeChecksumOnUpdate) {
                    ReplicaDescriptor handle =
                        _repository.openEntry(_pnfsId, EnumSet.of(OpenFlags.NOATIME));
                    try {
                        _checksumModule.verifyChecksum(handle);
                    } finally {
                        handle.close();
                    }
                }

                EntryState state = _repository.getState(_pnfsId);
                switch (state) {
                case CACHED:
                    if (_targetState == PRECIOUS) {
                        _repository.setState(_pnfsId, PRECIOUS);
                    }
                    // fall through
                case PRECIOUS:
                    for (StickyRecord record: _stickyRecords) {
                        _repository.setSticky(_pnfsId,
                                              record.owner(),
                                              record.expire(),
                                              false);
                    }
                    break;
                default:
                    finished(new CacheException("Cannot update file in state " + state));

                }
                finished(null);
            } catch (IOException e) {
                finished(new DiskErrorCacheException("I/O error during checksum calculation: " + e.getMessage()));
            } catch (InterruptedException e) {
                finished(new CacheException("Task was cancelled"));
            } catch (IllegalTransitionException e) {
                finished(new CacheException("Cannot update file in state " + e.getSourceState()));
            } catch (CacheException | NoSuchAlgorithmException | RuntimeException e) {
                finished(e);
            } finally {
                _requests.remove(_uuid);
            }
        }
    }
}
