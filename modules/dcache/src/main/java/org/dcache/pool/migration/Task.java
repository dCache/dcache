package org.dcache.pool.migration;

import com.google.common.collect.ImmutableList;
import statemap.TransitionUndefinedException;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import dmg.cells.nucleus.CellPath;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.services.pinmanager1.PinManagerMovePinMessage;
import org.dcache.util.FireAndForgetTask;
import org.dcache.util.ReflectionUtils;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Predicates.*;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Queues.newArrayDeque;

/**
 * Encapsulates the migration of a single replica of a migration job.
 *
 * A task encapsulates the logic for migrating a replica according to the conditions
 * defined by a migration job. The high level logic is defined in the Task state
 * machine generated with SMC, see Task.sm.
 *
 * This class mostly contains utility methods and auxiliary state used by the
 * state machine.
 */
public class Task
{
    private static final AtomicInteger _counter = new AtomicInteger();

    private final TaskContext _fsm;

    private final TaskCompletionHandler _callbackHandler;

    private final String _source;

    private final long _id;
    private final UUID _uuid;

    private final TaskParameters _parameters;
    private final PnfsId _pnfsId;
    private final EntryState _targetState;
    private final List<StickyRecord> _targetStickyRecords;
    private final List<StickyRecord> _pinsToMove;
    private final FileAttributes _fileAttributes;
    private final long _atime;

    private ScheduledFuture<?> _timerTask;
    private Deque<String> _locations = new ArrayDeque<>(0);
    private Set<String> _replicas = new HashSet<>();
    private CellPath _target;

    public Task(TaskParameters parameters,
                TaskCompletionHandler callbackHandler,
                String source,
                PnfsId pnfsId,
                EntryState targetState,
                List<StickyRecord> targetStickyRecords,
                List<StickyRecord> pinsToMove,
                FileAttributes fileAttributes,
                long atime)
    {
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

    public boolean getMustMovePins()
    {
        return !_pinsToMove.isEmpty();
    }

    public long getId()
    {
        return _id;
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    /** Time in milliseconds between pings. */
    public long getPingPeriod()
    {
        return _parameters.pool.getTimeoutInMillis() * 2;
    }

    /**
     * Time in milliseconds before we fail the task if we loose the
     * cell connection.
     */
    public long getNoResponseTimeout()
    {
        return _parameters.pool.getTimeoutInMillis() * 2;
    }

    /**
     * Time in milliseconds before we fail the task if we do not get
     * CopyFinished.
     */
    public long getTaskDeadTimeout()
    {
        return _parameters.pool.getTimeoutInMillis();
    }

    /**
     * Eager tasks copy files if attempts to update existing copies
     * timeout or fail due to communication problems. Other tasks fail
     * in this situation.
     */
    public boolean isEager()
    {
        return _parameters.isEager;
    }

    /**
     * Returns the current target pool, if any.
     */
    synchronized String getTarget()
    {
        return _target == null ? "" : _target.toSmallString();
    }

    /**
     * Returns a pool from the pool list using the pool selection
     * strategy.
     */
    private CellPath selectPool()
        throws NoSuchElementException
    {
        List<PoolManagerPoolInformation> pools =
                newArrayList(filter(_parameters.poolList.getPools(),
                                    compose(not(in(_replicas)), PoolManagerPoolInformation.GET_NAME)));
        PoolManagerPoolInformation pool = _parameters.selectionStrategy.select(pools);
        if (pool == null) {
            if (pools.isEmpty()) {
                throw new NoSuchElementException("No pools available.");
            }
            throw new NoSuchElementException("All target pools are full.");
        }
        return new CellPath(pool.getName());
    }


    /** Adds status information about the task to <code>pw</code>. */
    synchronized void getInfo(PrintWriter pw)
    {
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

    /** Message handler - ignores messages with the wrong ID */
    public synchronized void
        messageArrived(PoolMigrationCopyFinishedMessage message)
    {
        if (_uuid.equals(message.getUUID())) {
            _fsm.messageArrived(message);
        }
    }

    /** FSM Action */
    synchronized void queryLocations()
    {
        CellStub.addCallback(_parameters.pnfs.send(new PnfsGetCacheLocationsMessage(getPnfsId())),
                             new Callback<PnfsGetCacheLocationsMessage>("query_")
                             {
                                 @Override
                                 public void success(PnfsGetCacheLocationsMessage msg)
                                 {
                                     setLocations(msg.getCacheLocations());
                                     super.success(msg);
                                 }
                             }, _parameters.executor);
    }

    /**
     * Sets the list of pools on which a copy of the replica is known
     * to exist.
     */
    private synchronized void setLocations(List<String> locations)
    {
        Collection<String> onlinePools =
                transform(_parameters.poolList.getPools(), PoolManagerPoolInformation.GET_NAME);
        if (_parameters.isEager) {
            _locations = newArrayDeque(filter(locations, in(onlinePools)));
        } else {
            ImmutableList<String> offlinePools = _parameters.poolList.getOfflinePools();
            _locations = newArrayDeque(filter(locations, or(in(onlinePools), in(offlinePools))));
        }
    }

    /**
     * Returns true iff there are more target pools with copies of the
     * replica.
     */
    synchronized boolean hasMoreLocations()
    {
        return !_locations.isEmpty();
    }

    /**
     * Returns true iff the more replicas are needed to satisfy the requirements
     * of the migration job.
     */
    synchronized boolean needsMoreReplicas()
    {
        return _replicas.size() < _parameters.replicas;
    }

    /** FSM Action */
    synchronized void updateExistingReplica()
    {
        assert !_locations.isEmpty();

        initiateCopy(new CellPath(_locations.removeFirst()));
    }

    /** FSM Action */
    synchronized void initiateCopy()
    {
        try {
            initiateCopy(selectPool());
        } catch (NoSuchElementException e) {
            _target = null;
            _parameters.executor.execute(new FireAndForgetTask(new Runnable()
            {
                @Override
                public void run()
                {
                    synchronized (Task.this) {
                        _fsm.copy_nopools();
                    }
                }
            }));
        }
    }

    /**
     * Ask <code>target</code> to copy the file.
     */
    private synchronized void
        initiateCopy(CellPath target)
    {
        _target = target;
        PoolMigrationCopyReplicaMessage copyReplicaMessage =
                new PoolMigrationCopyReplicaMessage(_uuid,
                                                    _source,
                                                    _fileAttributes,
                                                    _targetState,
                                                    _targetStickyRecords,
                                                    _parameters.computeChecksumOnUpdate,
                                                    _parameters.forceSourceMode,
                                                    _parameters.maintainAtime ? _atime : null);
        CellStub.addCallback(_parameters.pool.send(_target, copyReplicaMessage),
                             new Callback<PoolMigrationCopyReplicaMessage>("copy_") {
                                 @Override
                                 public void success(PoolMigrationCopyReplicaMessage message)
                                 {
                                     _replicas.add(_target.getCellName());
                                     super.success(message);
                                 }
                             }, _parameters.executor);
    }

    /** FSM Action */
    synchronized void cancelCopy()
    {
        CellStub.addCallback(_parameters.pool.send(_target,
                                        new PoolMigrationCancelMessage(_uuid,
                                                                       _source,
                                                                       getPnfsId())),
                             new Callback<>("cancel_"), _parameters.executor);
    }

    /** FSM Action */
    synchronized void movePin()
    {
        Callback<PinManagerMovePinMessage> callback = new Callback<>("move_");
        String target = _target.getDestinationAddress().getCellName();
        PinManagerMovePinMessage message =
            new PinManagerMovePinMessage(getPnfsId(), _pinsToMove, _source, target);
        CellStub.addCallback(_parameters.pinManager.send(message), callback, _parameters.executor);
    }

    /** FSM Action */
    void notifyCancelled()
    {
        _parameters.executor.execute(new FireAndForgetTask(() -> _callbackHandler.taskCancelled(Task.this)));
    }

    /** FSM Action */
    void fail(int rc, String message)
    {
        _parameters.executor.execute(new FireAndForgetTask(() -> _callbackHandler.taskFailed(Task.this, rc, message)));
    }

    /** FSM Action */
    void failPermanently(int rc, String message)
    {
        _parameters.executor.execute(new FireAndForgetTask(
                () -> _callbackHandler.taskFailedPermanently(Task.this, rc, message)));
    }

    /** FSM Action */
    void notifyCompleted()
    {
        _parameters.executor.execute(new FireAndForgetTask(() -> _callbackHandler.taskCompleted(Task.this)));
    }

    /** FSM Action */
    synchronized void startTimer(long delay)
    {
        Runnable task =
            new Runnable()
            {
                @Override
                public void run()
                {
                    synchronized (Task.this) {
                        if (_timerTask != null) {
                            _fsm.timer();
                            _timerTask = null;
                        }
                    }
                }
            };
        _timerTask =
            _parameters.executor.schedule(new FireAndForgetTask(task),
                               delay, TimeUnit.MILLISECONDS);
    }

    /** FSM Action */
    synchronized void stopTimer()
    {
        if (_timerTask != null) {
            _timerTask.cancel(false);
            _timerTask = null;
        }
    }

    /** FSM Action */
    synchronized void ping()
    {
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
    public synchronized void run()
    {
        _fsm.run();
    }

    /**
     * Cancels the task, if not already completed. This will trigger a
     * notification (postponed).
     */
    public synchronized void cancel()
    {
        _fsm.cancel();
    }

    /**
     * Helper class implementing the MessageCallback interface,
     * forwarding all messages as events to the state machine. Events
     * are forwarded via an executor to guarantee asynchronous delivery
     * (SMC state machines do not allow transitions to be triggered
     * from within transitions).
     */
    class Callback<T extends Message> extends AbstractMessageCallback<T>
    {
        private final String _prefix;

        public Callback()
        {
            _prefix = "";
        }

        public Callback(String prefix)
        {
            _prefix = prefix;
        }

        protected void transition(String name, final Object... arguments)
        {
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
            } catch (Throwable e) {
                Thread thisThread = Thread.currentThread();
                Thread.UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
                ueh.uncaughtException( thisThread, e);
            }
        }

        @Override
        public void success(T message)
        {
            transition("success");
        }

        @Override
        public void failure(int rc, Object cause)
        {
            transition("failure", rc, cause);
        }

        @Override
        public void timeout(String error)
        {
            transition("timeout");
        }

        @Override
        public void noroute(CellPath path)
        {
            transition("noroute");
        }
    }
}
