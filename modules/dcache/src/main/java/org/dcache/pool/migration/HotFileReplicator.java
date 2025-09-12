package org.dcache.pool.migration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.dcache.cells.CellStub;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolIoFileMessage;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.command.Command;
import dmg.util.command.Option;

public class HotFileReplicator implements CellMessageReceiver, CellCommandListener, CellSetupProvider,
      CellLifeCycleAware, TaskCompletionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HotFileReplicator.class);

    private static final int DEFAULT_CONCURRENCY = 1;
    private static final int DEFAULT_REPLICAS = 10;
    private static final long DEFAULT_THRESHOLD = 5;

    // TODO: either use concurrency to manage number of Tasks in flight, or remove it.
    private int concurrency = DEFAULT_CONCURRENCY;
    private int replicas = DEFAULT_REPLICAS;
    private long threshold = DEFAULT_THRESHOLD;

    private final Map<PnfsId, Task> _inFlightMigrations = new HashMap<>();
    private final Lock _lock = new ReentrantLock(true);
    private final MigrationContext _context;

    HotFileReplicator(MigrationContext context) {
        _context = context;
    }

    @Override
    public CellSetupProvider mock() {
        return new HotFileReplicator(new MigrationContextDecorator(_context) {
            @Override
            public boolean lock(PnfsId pnfsId) {
                return false;
            }

            @Override
            public void unlock(PnfsId pnfsId) {
            }

            @Override
            public boolean isActive(PnfsId pnfsId) {
                return true;
            }
        });
    }

    void messageArrived(PoolIoFileMessage message) {
        _inFlightMigrations.remove(message.getPnfsId());
    }

    public void maybeReplicate(PoolIoFileMessage message, long numberOfRequests) {
        _lock.lock();
        try {
            PnfsId pnfsId = message.getPnfsId();
            LOGGER.debug("maybeReplicate() logging {} requests for pnfsId {} (threshold {})", numberOfRequests, pnfsId, threshold);
            if (numberOfRequests < threshold || _inFlightMigrations.containsKey(pnfsId))
                return;
            // Assemble the correct information, and start the task
            try {
                LOGGER.debug("maybeReplicate() initiating request for {} replicas of pnfsId {}", replicas, pnfsId);
                Repository repository = _context.getRepository();
                CacheEntry entry = repository.getEntry(pnfsId);
                TaskParameters taskParameters = new TaskParameters(_context.getPoolStub(),
                      _context.getPnfsStub(),
                      _context.getPinManagerStub(),
                      _context.getExecutor(),
                      new ProportionalPoolSelectionStrategy(),
                      new PoolListByPoolGroupOfPool(_context.getPoolManagerStub(), _context.getPoolName()),
                      false,
                      false,
                      false,
                      false,
                      true,
                      replicas,
                      false);

                Task task = new Task(taskParameters, this, _context.getPoolName(),
                      entry.getPnfsId(),
                      ReplicaState.CACHED, Collections.emptyList(),
                      Collections.emptyList(), entry.getFileAttributes(), entry.getLastAccessTime());
                _inFlightMigrations.put(message.getPnfsId(), task);
                LOGGER.debug("maybeReplicate() scheduling migration task for pnfsId {} on executor", pnfsId);
                taskParameters.executor.execute(() -> {
                    LOGGER.debug("maybeReplicate() migration task for pnfsId {} started on executor", pnfsId);
                    task.run();
                });
                LOGGER.debug("maybeReplicate() migration task for pnfsId {} submitted to executor", pnfsId);
            } catch (FileNotInCacheException e) {
                LOGGER.warn("File no longer in cache for pnfsId {}: {}", pnfsId, e.toString());
            } catch (CacheException e) {
                LOGGER.error("CacheException for pnfsId {}: {}", pnfsId, e.toString());
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while replicating pnfsId {}: {}", pnfsId, e.toString());
                Thread.currentThread().interrupt();
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
        LOGGER.info("Task cancelled for pnfsId {}", task.getPnfsId());
        _dropTask(task);
    }

    @Override
    public void taskFailed(Task task, int rc, String msg) {
        LOGGER.warn("Task failed for pnfsId {}: rc={}, msg={}", task.getPnfsId(), rc, msg);
        _dropTask(task);
    }

    @Override
    public void taskFailedPermanently(Task task, int rc, String msg) {
        LOGGER.error("Task permanently failed for pnfsId {}: rc={}, msg={}", task.getPnfsId(), rc, msg);
        _dropTask(task);
    }

    @Override
    public void taskCompleted(Task task) {
        LOGGER.info("Task completed for pnfsId {}", task.getPnfsId());
        _dropTask(task);
    }

    @Override
    public void taskCompletedWithNote(Task task, String msg) {
        LOGGER.info("Task completed with note for pnfsId {}: {}", task.getPnfsId(), msg);
        TaskCompletionHandler.super.taskCompletedWithNote(task, msg);
    }

    /**
     * Command to get or set the number of replicas.
     */
    @Command(name = "hotfile-replicator replicas",
             description = "Get or set the number of replicas to ensure via replication.")
    public class NumReplicasCommand implements Callable<String> {
        @Option(name = "set", usage = "Set the number of replicas.")
        Integer set;

        @Override
        public String call() {
            if (set != null) {
                setNumReplicas(set);
                return "NumReplicas set to " + set;
            }
            return "Current replicas: " + getNumReplicas();
        }
    }

    /**
     * Command to get or set the threshold.
     */
    @Command(name = "hotfile-replicator threshold",
             description = "Get or set the threshold for triggering replication.")
    public class ThresholdCommand implements Callable<String> {
        @Option(name = "set", usage = "Set the threshold value.")
        Long set;

        @Override
        public String call() {
            if (set != null) {
                setThreshold(set);
                return "Threshold set to " + set;
            }
            return "Current threshold: " + getThreshold();
        }
    }

    /**
     * Command to get or set the concurrency value.
     */
    @Command(name = "hotfile-replicator concurrency",
             description = "Get or set the concurrency for hot file replication.")
    public class ConcurrencyCommand implements Callable<String> {
        @Option(name = "set", usage = "Set the concurrency value.")
        Integer set;

        @Override
        public String call() {
            if (set != null) {
                setConcurrency(set);
                return "Concurrency set to " + set;
            }
            return "Current concurrency: " + getConcurrency();
        }
    }

    /**
     * Command to print information about in-flight migration tasks.
     */
    @Command(name = "hotfile-replicator tasks",
             description = "Show information about migration tasks currently in flight.")
    public class TasksCommand implements Callable<String> {
        @Override
        public String call() {
            StringBuilder sb = new StringBuilder();
            _lock.lock();
            try {
                if (_inFlightMigrations.isEmpty()) {
                    sb.append("No migration tasks in flight.\n");
                } else {
                    sb.append("In-flight migration tasks:\n");
                    for (Map.Entry<PnfsId, Task> entry : _inFlightMigrations.entrySet()) {
                        sb.append("  PnfsId: ").append(entry.getKey()).append("\n");
                        sb.append("    Task: ").append(entry.getValue()).append("\n");
                    }
                }
            } finally {
                _lock.unlock();
            }
            return sb.toString();
        }
    }

    // Accessors and mutators for command use
    public int getNumReplicas() {
        _lock.lock();
        try {
            return replicas;
        } finally {
            _lock.unlock();
        }
    }
    public void setNumReplicas(int value) {
        _lock.lock();
        try {
            replicas = value;
            LOGGER.info("NumReplicas updated to {}", value);
        } finally {
            _lock.unlock();
        }
    }
    public long getThreshold() {
        _lock.lock();
        try {
            return threshold;
        } finally {
            _lock.unlock();
        }
    }
    public void setThreshold(long value) {
        _lock.lock();
        try {
            threshold = value;
            LOGGER.info("Threshold updated to {}", value);
        } finally {
            _lock.unlock();
        }
    }
    public int getConcurrency() {
        _lock.lock();
        try {
            return concurrency;
        } finally {
            _lock.unlock();
        }
    }
    public synchronized void setConcurrency(int value) {
        concurrency = value;
        LOGGER.info("Concurrency updated to {}", value);
    }
}
