package org.dcache.pool.migration;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import dmg.cells.nucleus.CellPath;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.services.pinmanager1.PinManagerMovePinMessage;
import org.dcache.util.FireAndForgetTask;
import org.dcache.util.ReflectionUtils;
import org.dcache.vehicles.FileAttributes;
import statemap.TransitionUndefinedException;

/**
 * Encapsulates the migration of a single replica of a migration job.
 * <p>
 * A task encapsulates the logic for migrating a replica according to the conditions defined by a
 * migration job. The high level logic is defined in the Task state machine generated with SMC, see
 * Task.sm.
 * <p>
 * This class mostly contains utility methods and auxiliary state used by the state machine.
 */
public class Task {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Task.class);

    private static final AtomicInteger _counter = new AtomicInteger();

    private final TaskContext _fsm;

    private final TaskCompletionHandler _callbackHandler;

    private final String _source;

    private final long _id;
    private final UUID _uuid;

    private final TaskParameters _parameters;
    private final PnfsId _pnfsId;
    private final ReplicaState _targetState;
    private final List<StickyRecord> _targetStickyRecords;
    private final List<StickyRecord> _pinsToMove;
    private final FileAttributes _fileAttributes;
    private final long _atime;

    private ScheduledFuture<?> _timerTask;
    private Deque<String> _locations = new ArrayDeque<>(0);
    private final Set<String> _replicas = new HashSet<>();
    private CellPath _target;
    private Optional<String> _cancelReason = Optional.empty();

    public Task(TaskParameters parameters,
          TaskCompletionHandler callbackHandler,
          String source,
          PnfsId pnfsId,
          ReplicaState targetState,
          List<StickyRecord> targetStickyRecords,
          List<StickyRecord> pinsToMove,
          FileAttributes fileAttributes,
          long atime) {
        _parameters = parameters;
        _pnfsId = pnfsId;
        _targetState = targetState;
        _targetStickyRecords = targetStickyRecords;
        _pinsToMove = pinsToMove;
        _fileAttributes = fileAttributes;
        _id = _counter.getAndIncrement();
        _uuid = UUID.randomUUID();
        _fsm = new TaskContext(this);
        _callbackHandler = callbackHandler;
        _source = source;
        _atime = atime;
    }

    public boolean getMustMovePins() {
        return !_pinsToMove.isEmpty();
    }

    public long getId() {
        return _id;
    }

    public PnfsId getPnfsId() {
        return _pnfsId;
    }

    /**
     * Time in milliseconds between pings.
     */
    public long getPingPeriod() {
        return _parameters.pool.getTimeoutInMillis() * 2;
    }

    /**
     * Time in milliseconds before we fail the task if we loose the cell connection.
     */
    public long getNoResponseTimeout() {
        return _parameters.pool.getTimeoutInMillis() * 2;
    }

    /**
     * Time in milliseconds before we fail the task if we do not get CopyFinished.
     */
    public long getTaskDeadTimeout() {
        return _parameters.pool.getTimeoutInMillis();
    }

    /**
     * Eager tasks copy files if attempts to update existing copies timeout or fail due to
     * communication problems. Other tasks fail in this situation.
     */
    public boolean isEager() {
        return _parameters.isEager;
    }

    /**
     * Meta only jobs only upgrade existing replicas - they never copy replicas. If no or not enough
     * existing replicas exist, the task fails permanently.
     */
    public boolean isMetaOnly() {
        return _parameters.isMetaOnly;
    }

    /**
     * Returns the current target pool, if any.
     */
    synchronized String getTarget() {
        return _target == null ? "" : _target.toSmallString();
    }

    /**
     * Returns a pool from the pool list using the pool selection strategy.
     */
    private CellPath selectPool()
          throws NoSuchElementException {
        LOGGER.debug("Task.selectPool() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        LOGGER.debug("poollist.getPools() = {}", _parameters.poolList.getPools().stream().map(PoolManagerPoolInformation::getName).reduce((a, b) -> a + "," + b).orElse(""));
        List<PoolManagerPoolInformation> pools =
              _parameters.poolList.getPools().stream()
                    .filter(pool -> !_replicas.contains(pool.getName()))
                    .collect(toList());
        PoolManagerPoolInformation pool = _parameters.selectionStrategy.select(pools);
        if (pool == null) {
            if (pools.isEmpty()) {
                throw new NoSuchElementException("No pools available.");
            }
            throw new NoSuchElementException("All target pools are full.");
        }
        return new CellPath(pool.getName());
    }


    /**
     * Adds status information about the task to <code>pw</code>.
     */
    synchronized void getInfo(PrintWriter pw) {
        if (_target != null) {
            pw.println(String.format("[%d] %s: %s -> %s",
                  _id,
                  _pnfsId,
                  _fsm.getState(),
                  _target.toSmallString()));
        } else {
            pw.println(String.format("[%d] %s: %s",
                  _id,
                  _pnfsId,
                  _fsm.getState()));
        }
    }

    /**
     * Message handler - ignores messages with the wrong ID
     */
    public synchronized void
    messageArrived(PoolMigrationCopyFinishedMessage message) {
        if (_uuid.equals(message.getUUID())) {
            _replicas.add(getTarget());
            _fsm.messageArrived(message);
        }
    }

    /**
     * FSM Action
     */
    synchronized void queryLocations() {
        LOGGER.debug("Task.queryLocations() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        CellStub.addCallback(_parameters.pnfs.send(new PnfsGetCacheLocationsMessage(getPnfsId())),
              new Callback<PnfsGetCacheLocationsMessage>("query_") {
                  @Override
                  public void success(PnfsGetCacheLocationsMessage msg) {
                      setLocations(msg.getCacheLocations());
                      super.success(msg);
                  }
              }, _parameters.executor);
    }

    /**
     * Sets the list of pools on which a copy of the replica is known to exist.
     */
    private synchronized void setLocations(Collection<String> locations) {
        LOGGER.debug("Task.setLocations() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        Stream<String> pools = _parameters.poolList.getPools().stream()
              .map(PoolManagerPoolInformation::getName);
        if (!_parameters.isEager) {
            pools = Stream.concat(pools, _parameters.poolList.getOfflinePools().stream());
        }
        _locations = pools.filter(locations::contains).collect(toCollection(ArrayDeque::new));
    }

    /**
     * Returns true iff there are more target pools with copies of the replica.
     */
    synchronized boolean hasMoreLocations() {
        LOGGER.debug("Task.hasMoreLocations() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        return !_locations.isEmpty();
    }

    /**
     * Returns true iff the more replicas are needed to satisfy the requirements of the migration
     * job.
     */
    synchronized boolean needsMoreReplicas() {
        LOGGER.debug("Task.needsMoreReplicas() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        return _replicas.size() < _parameters.replicas;
    }

    synchronized boolean moreReplicasPossible() {
        LOGGER.debug("Task.moreReplicasPossible() called for pnfsId {} (taskId={})", getPnfsId(), getId());
      return _parameters.waitForTargets || (_replicas.size() < _parameters.poolList.getPools().size());
    }

    /**
     * FSM Action
     */
    synchronized void updateExistingReplica() {
        LOGGER.debug("Task.updateExistingReplica() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        assert !_locations.isEmpty();

        initiateCopy(new CellPath(_locations.removeFirst()));
    }

    /**
     * FSM Action
     */
    synchronized void initiateCopy() {
        LOGGER.debug("Task.initiateCopy() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        checkState(!isMetaOnly());

        try {
            CellPath selectedPool = selectPool();
            LOGGER.debug("Task.initiateCopy() has selected pool {} to copy pnfsID {}", selectedPool, getPnfsId());
            initiateCopy(selectedPool);
        } catch (NoSuchElementException e) {
            _target = null;
            _parameters.executor.execute(new FireAndForgetTask(() -> {
                synchronized (Task.this) {
                    _fsm.copy_nopools();
                }
            }));
        }
    }

    /**
     * Ask <code>target</code> to copy the file.
     */
    private synchronized void
    initiateCopy(CellPath target) {
        LOGGER.debug("Task.initiateCopy(target) called for pnfsId {} (taskId={}) target={}", getPnfsId(), getId(), target);
        _target = target;
        PoolMigrationCopyReplicaMessage copyReplicaMessage =
              new PoolMigrationCopyReplicaMessage(_uuid,
                    _source,
                    _fileAttributes,
                    _targetState,
                    _targetStickyRecords,
                    _parameters.computeChecksumOnUpdate,
                    _parameters.forceSourceMode,
                    _parameters.maintainAtime ? _atime : null,
                    _parameters.isMetaOnly);
        CellStub.addCallback(_parameters.pool.send(_target, copyReplicaMessage),
              new Callback<>("copy_"), _parameters.executor);
    }

    /**
     * FSM Action
     */
    synchronized void cancelCopy() {
        LOGGER.debug("Task.cancelCopy() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        cancelCopy(_cancelReason.orElse(null));
    }

    synchronized void cancelCopy(String reason) {
        LOGGER.debug("Task.cancelCopy(reason) called for pnfsId {} (taskId={}) reason={}", getPnfsId(), getId(), reason);
        CellStub.addCallback(_parameters.pool.send(_target,
                    new PoolMigrationCancelMessage(_uuid,
                          _source,
                          getPnfsId(),
                          reason)),
              new Callback<>("cancel_"), _parameters.executor);
    }

    /**
     * FSM Action
     */
    synchronized void movePin() {
        LOGGER.debug("Task.movePin() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        Callback<PinManagerMovePinMessage> callback = new Callback<>("move_");
        String target = _target.getDestinationAddress().getCellName();
        PinManagerMovePinMessage message =
              new PinManagerMovePinMessage(getPnfsId(), _pinsToMove, _source, target);
        CellStub.addCallback(_parameters.pinManager.send(message), callback, _parameters.executor);
    }

    /**
     * FSM Action
     */
    void notifyCancelled() {
        LOGGER.debug("Task.notifyCancelled() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        _parameters.executor.execute(
              new FireAndForgetTask(() -> _callbackHandler.taskCancelled(Task.this)));
    }

    /**
     * FSM Action
     */
    void fail(int rc, String message) {
        LOGGER.debug("Task.fail() called for pnfsId {} (taskId={}) rc={} message={}", getPnfsId(), getId(), rc, message);
        _parameters.executor.execute(
              new FireAndForgetTask(() -> _callbackHandler.taskFailed(Task.this, rc, message)));
    }

    /**
     * FSM Action
     */
    void failPermanently(int rc, String message) {
        LOGGER.debug("Task.failPermanently() called for pnfsId {} (taskId={}) rc={} message={}", getPnfsId(), getId(), rc, message);
        _parameters.executor.execute(new FireAndForgetTask(
              () -> _callbackHandler.taskFailedPermanently(Task.this, rc, message)));
    }

    /**
     * FSM Action
     */
    void notifyCompleted() {
        LOGGER.debug("Task.notifyCompleted() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        _parameters.executor.execute(
              new FireAndForgetTask(() -> _callbackHandler.taskCompleted(Task.this)));
    }

    /**
     * FSM Action
     */
    void notifyCompletedWithInsufficientReplicas() {
        LOGGER.debug("Task.notifyCompletedWithInsufficientReplicas() called for pnfsId {} (taskId={})", getPnfsId(), getId());
      _parameters.executor.execute(
              new FireAndForgetTask(() -> _callbackHandler.taskCompletedWithNote(Task.this,
                      String.format("File replicas truncated at %s due to pool availability (%s requested)",
                                    _replicas.size(), _parameters.replicas))));
    }

    /**
     * FSM Action
     */
    synchronized void startTimer(long delay) {
        LOGGER.debug("Task.startTimer() called for pnfsId {} (taskId={}) delay={}ms", getPnfsId(), getId(), delay);
        Runnable task =
              () -> {
                  synchronized (Task.this) {
                      if (_timerTask != null) {
                          _fsm.timer();
                          _timerTask = null;
                      }
                  }
              };
        _timerTask =
              _parameters.executor.schedule(new FireAndForgetTask(task),
                    delay, TimeUnit.MILLISECONDS);
    }

    /**
     * FSM Action
     */
    synchronized void stopTimer() {
        LOGGER.debug("Task.stopTimer() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        if (_timerTask != null) {
            _timerTask.cancel(false);
            _timerTask = null;
        }
    }

    /**
     * FSM Action
     */
    synchronized void ping() {
        LOGGER.debug("Task.ping() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        CellStub.addCallback(_parameters.pool.send(_target,
                    new PoolMigrationPingMessage(_uuid,
                          _source,
                          getPnfsId())),
              new Callback<>("ping_"),
              _parameters.executor);
    }

    /**
     * Starts the task.
     */
    public synchronized void run() {
        LOGGER.debug("Task.run() called for pnfsId {} (taskId={})", getPnfsId(), getId());
        try {
            if (_fileAttributes.isDefined(FileAttribute.LOCATIONS)) {
                LOGGER.debug("Task.run() LOCATIONS found for pnfsId {} (taskId={})", getPnfsId(), getId());
                setLocations(_fileAttributes.getLocations());
                LOGGER.debug("Task.run() calling startWithLocations() for pnfsId {} (taskId={})", getPnfsId(), getId());
                _fsm.startWithLocations();
            } else {
                LOGGER.debug("Task.run() calling startWithoutLocations() for pnfsId {} (taskId={})", getPnfsId(), getId());
                _fsm.startWithoutLocations();
            }
        } catch (Exception e) {
            LOGGER.error("Exception in Task.run() for pnfsId {} (taskId={}): {}", getPnfsId(), getId(), e.toString(), e);
            // Optionally, fail the task explicitly if needed:
            if (_callbackHandler != null) {
                _callbackHandler.taskFailed(this, -1, "Exception in Task.run(): " + e.toString());
            }
        }
        LOGGER.debug("Task.run() exiting for (taskId={})", getId());
    }

    /**
     * Cancels the task, if not already completed. This will trigger a notification (postponed).
     */
    public synchronized void cancel(String why) {
        assert !_cancelReason.isPresent();
        _cancelReason = Optional.of(why);
        _fsm.cancel();
    }

    public synchronized String getCancelReason() {
        return _cancelReason.orElse("an unknown reason");
    }

    /**
     * Helper class implementing the MessageCallback interface, forwarding all messages as events to
     * the state machine. Events are forwarded via an executor to guarantee asynchronous delivery
     * (SMC state machines do not allow transitions to be triggered from within transitions).
     */
    class Callback<T extends Message> extends AbstractMessageCallback<T> {

        private final String _prefix;

        public Callback() {
            _prefix = "";
        }

        public Callback(String prefix) {
            _prefix = prefix;
        }

        protected void transition(String name, final Object... arguments) {
            LOGGER.debug("Callback.transition() ENTRY: prefix='{}', name='{}', arguments={}, pnfsId={}, taskId={}", _prefix, name, java.util.Arrays.toString(arguments), Task.this.getPnfsId(), Task.this.getId());
            try {
                Class<?>[] parameterTypes = new Class[arguments.length];
                for (int i = 0; i < arguments.length; i++) {
                    parameterTypes[i] = arguments[i].getClass();
                }
                final Method m =
                      ReflectionUtils.resolve(_fsm.getClass(), _prefix + name,
                            parameterTypes);
                if (m != null) {
                    try {
                        synchronized (Task.this) {
                            m.invoke(_fsm, arguments);
                        }
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        /* We are not allowed to call this
                         * method. Better escalate it.
                         */
                        throw new RuntimeException("Bug detected", e);
                    } catch (TransitionUndefinedException e) {
                        throw new RuntimeException("State machine is incomplete", e);
                    }
                }
                LOGGER.debug("Callback.transition() EXIT: prefix='{}', name='{}', arguments={}, pnfsId={}, taskId={}", _prefix, name, java.util.Arrays.toString(arguments), Task.this.getPnfsId(), Task.this.getId());
            } catch (Throwable e) {
                Thread thisThread = Thread.currentThread();
                Thread.UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
                ueh.uncaughtException(thisThread, e);
            }
        }

        @Override
        public void success(T message) {
            LOGGER.debug("Callback.success() ENTRY: message={}, pnfsId={}, taskId={}", message, Task.this.getPnfsId(), Task.this.getId());
            transition("success");
            LOGGER.debug("Callback.success() EXIT: message={}, pnfsId={}, taskId={}", message, Task.this.getPnfsId(), Task.this.getId());
        }

        @Override
        public void failure(int rc, Object cause) {
            LOGGER.debug("Callback.failure() ENTRY: rc={}, cause={}, pnfsId={}, taskId={}", rc, cause, Task.this.getPnfsId(), Task.this.getId());
            transition("failure", rc, cause);
            LOGGER.debug("Callback.failure() EXIT: rc={}, cause={}, pnfsId={}, taskId={}", rc, cause, Task.this.getPnfsId(), Task.this.getId());
        }

        @Override
        public void timeout(String error) {
            LOGGER.debug("Callback.timeout() ENTRY: error={}, pnfsId={}, taskId={}", error, Task.this.getPnfsId(), Task.this.getId());
            transition("timeout");
            LOGGER.debug("Callback.timeout() EXIT: error={}, pnfsId={}, taskId={}", error, Task.this.getPnfsId(), Task.this.getId());
        }

        @Override
        public void noroute(CellPath path) {
            LOGGER.debug("Callback.noroute() ENTRY: path={}, pnfsId={}, taskId={}", path, Task.this.getPnfsId(), Task.this.getId());
            transition("noroute");
            LOGGER.debug("Callback.noroute() EXIT: path={}, pnfsId={}, taskId={}", path, Task.this.getPnfsId(), Task.this.getId());
        }
    }
}
