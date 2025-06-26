package org.dcache.pool.migration;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolIoFileMessage;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellSetupProvider;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.dcache.cells.CellStub;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;

public class HotFileReplicator implements CellMessageReceiver, CellCommandListener, CellSetupProvider,
      CellLifeCycleAware, TaskCompletionHandler {

    private static final int defaultNumReplicas = 10;
    private static final int defaultHotspotThreshold = 5;
    private static final int defaultConcurrency = 1;

    // TODO: either use concurrency to manage number of Tasks in flight, or remove it.
    private final int concurrency = defaultConcurrency;
    private final int _numReplicas = defaultNumReplicas;
    private final long _hotspotThreshold = defaultHotspotThreshold;

    private final Map<PnfsId, Task> _inFlightMigrations = new HashMap<>();
    private final Lock _lock = new ReentrantLock(true);
    private final MigrationContext _context;
    private final RefreshablePoolList _poolList;
    private final TaskParameters _taskParameters;

    /**
     * Test-only constructor for unit testing. Initializes with provided context, poolList, and taskParameters.
     */
    HotFileReplicator(MigrationContext context, RefreshablePoolList poolList, TaskParameters taskParameters) {
        _context = context;
        _poolList = poolList;
        _taskParameters = taskParameters;
    }

    private HotFileReplicator(MigrationContext context, boolean real) {
        _context = context;
        CellStub poolManager = _context.getPoolManagerStub();
        _poolList = new PoolListByPoolGroupOfPool(poolManager, _context.getPoolName());
        _taskParameters = new TaskParameters(context.getPoolStub(),
              context.getPnfsStub(),
              context.getPinManagerStub(),
              context.getExecutor(),
              new ProportionalPoolSelectionStrategy(),
              _poolList,
              false,
              false,
              false,
              false,
              true,
              _numReplicas,
              false);
    }

    void messageArrived(PoolIoFileMessage message) {
        _inFlightMigrations.remove(message.getPnfsId());
    }

    public void maybeReplicate(PoolIoFileMessage message, long numberOfRequests) {
        _lock.lock();
        try {
            PnfsId pnfsId = message.getPnfsId();
            if (numberOfRequests < _hotspotThreshold || _inFlightMigrations.containsKey(pnfsId))
                return;
            // Assemble the correct information, and start the task
            try {
                Repository repository = _context.getRepository();
                CacheEntry entry = repository.getEntry(pnfsId);

                Task task = new Task(_taskParameters, this, _context.getPoolName(),
                      entry.getPnfsId(),
                      ReplicaState.CACHED, Collections.emptyList(),
                      Collections.emptyList(), entry.getFileAttributes(), entry.getLastAccessTime());
                _inFlightMigrations.put(message.getPnfsId(), task);
                task.run();
            } catch (FileNotInCacheException e) {
                // TODO log or failure?
            } catch (CacheException e) {
                // TODO Log
            } catch (InterruptedException e) {
                // TODO Log
            }
        } finally {
            _lock.unlock();
        }
    }

    private void _dropTask(Task task) {
        _lock.lock();
        try {
            PnfsId pnfsId = task.getPnfsId();
            if (_inFlightMigrations.containsKey(pnfsId)) {
                _inFlightMigrations.remove(pnfsId);
                _context.unlock(pnfsId);
            }
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public void taskCancelled(Task task) {
        // TODO Log
        _dropTask(task);
    }

    @Override
    public void taskFailed(Task task, int rc, String msg) {
        // TODO Log
        _dropTask(task);
    }

    @Override
    public void taskFailedPermanently(Task task, int rc, String msg) {
        // TODO Log
        _dropTask(task);
    }

    @Override
    public void taskCompleted(Task task) {
        // TODO Log
        _dropTask(task);
    }

    @Override
    public void taskCompletedWithNote(Task task, String msg) {
        // TODO Decide if we're handling notes.
        TaskCompletionHandler.super.taskCompletedWithNote(task, msg);
    }
}
