package org.dcache.pool.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import statemap.TransitionUndefinedException;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
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
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.services.pinmanager1.PinManagerMovePinMessage;
import org.dcache.util.ReflectionUtils;
import org.dcache.vehicles.FileAttributes;

public class Task
{
    private final static Logger _log = LoggerFactory.getLogger(Job.class);
    private final static AtomicInteger _counter = new AtomicInteger();

    private final TaskContext _fsm;

    private final Job _job;

    private final CellStub _pool;
    private final CellStub _pnfs;
    private final CellStub _pinManager;
    private final ScheduledExecutorService _executor;
    private final String _source;
    private final String _pinPrefix;
    private final JobDefinition _definition;

    private final long _id;
    private final UUID _uuid;

    private final CacheEntry _entry;

    private ScheduledFuture<?> _timerTask;
    private List<String> _locations = Collections.emptyList();
    private CellPath _target;

    public Task(Job job,
                CellStub pool,
                CellStub pnfs,
                CellStub pinManager,
                ScheduledExecutorService executor,
                String source,
                CacheEntry entry,
                JobDefinition definition)
    {
        _id = _counter.getAndIncrement();
        _uuid = UUID.randomUUID();
        _fsm = new TaskContext(this);
        _job = job;
        _pool = pool;
        _pnfs = pnfs;
        _pinManager = pinManager;
        _executor = executor;
        _source = source;
        _entry = entry;
        _definition = definition;
        _pinPrefix =
            _pinManager.getDestinationPath().getDestinationAddress().getCellName();
    }

    public boolean getMustMovePins()
    {
        return _definition.mustMovePins;
    }

    public long getId()
    {
        return _id;
    }

    public PnfsId getPnfsId()
    {
        return _entry.getPnfsId();
    }

    public long getFileSize()
    {
        return _entry.getReplicaSize();
    }

    /** Time in milliseconds between pings. */
    public long getPingPeriod()
    {
        return _pool.getTimeoutInMillis() * 2;
    }

    /**
     * Time in milliseconds before we fail the task if we loose the
     * cell connection.
     */
    public long getNoResponseTimeout()
    {
        return _pool.getTimeoutInMillis() * 2;
    }

    /**
     * Time in milliseconds before we fail the task if we do not get
     * CopyFinished.
     */
    public long getTaskDeadTimeout()
    {
        return _pool.getTimeoutInMillis();
    }

    /**
     * Eager tasks copy files if attempts to update existing copies
     * timeout or fail due to communication problems. Other tasks fail
     * in this situation.
     */
    public boolean isEager()
    {
        return _definition.isEager;
    }

    /**
     * Returns the current target pool, if any.
     */
    synchronized String getTarget()
    {
        return _target == null ? "" : _target.toSmallString();
    }

    /**
     * Returns true if and only if <code>record</code> is owned by the
     * pin manager.
     */
    private boolean isPin(StickyRecord record)
    {
        return record.owner().startsWith(_pinPrefix);
    }

    /** Returns the intended entry state of the target replica. */
    private EntryState getTargetState()
    {
        switch (_definition.targetMode.state) {
        case SAME:
            return _entry.getState();
        case CACHED:
            return EntryState.CACHED;
        case PRECIOUS:
            return EntryState.PRECIOUS;
        default:
            throw new IllegalStateException("Unsupported target mode");
        }
    }

    /** Returns the intended sticky records of the target replica. */
    private List<StickyRecord> getTargetStickyRecords()
    {
        List<StickyRecord> result = new ArrayList();
        if (_definition.targetMode.state == CacheEntryMode.State.SAME) {
            for (StickyRecord record: _entry.getStickyRecords()) {
                if (!isPin(record)) {
                    result.add(record);
                }
            }
        }
        result.addAll(_definition.targetMode.stickyRecords);
        return result;
    }

    /**
     * Returns the pool names in the associated pool list.
     */
    private Collection<String> getPools()
    {
        Collection<String> pools = new HashSet<>();
        for (PoolManagerPoolInformation pool: _definition.poolList.getPools()) {
            pools.add(pool.getName());
        }
        return pools;
    }

    /**
     * Returns a pool from the pool list using the pool selection
     * strategy.
     */
    private CellPath selectPool()
        throws NoSuchElementException
    {
        List<PoolManagerPoolInformation> pools =
            _definition.poolList.getPools();
        if (pools.isEmpty()) {
            throw new NoSuchElementException("No pools available");
        }
        return new CellPath(_definition.selectionStrategy.select(pools).getName());
    }


    /** Adds status information about the task to <code>pw</code>. */
    synchronized void getInfo(PrintWriter pw)
    {
        if (_target != null) {
            pw.println(String.format("[%d] %s: %s -> %s",
                                     _id,
                                     _entry.getPnfsId(),
                                     _fsm.getState(),
                                     _target.toSmallString()));
        } else {
            pw.println(String.format("[%d] %s: %s",
                                     _id,
                                     _entry.getPnfsId(),
                                     _fsm.getState()));
        }
    }

    /** Message handler - ignores messages with the wrong ID */
    synchronized public void
        messageArrived(PoolMigrationCopyFinishedMessage message)
    {
        if (_uuid.equals(message.getUUID())) {
            _fsm.messageArrived(message);
        }
    }

    /** FSM Action */
    synchronized void queryLocations()
    {
        _pnfs.send(new PnfsGetCacheLocationsMessage(getPnfsId()),
                   PnfsGetCacheLocationsMessage.class,
                   new Callback<PnfsGetCacheLocationsMessage>("query_")
                   {
                       @Override
                       public void success(PnfsGetCacheLocationsMessage msg)
                       {
                           setLocations(msg.getCacheLocations());
                           super.success(msg);
                       }
                   });
    }

    /**
     * Sets the list of pools on which a copy of the replica is known
     * to exist.
     */
    private synchronized void setLocations(List<String> locations)
    {
        _locations = new ArrayList(locations);
        _locations.retainAll(getPools());
    }

    /**
     * Returns true iff there are more target pools with copies of the
     * replica.
     */
    synchronized boolean hasMoreLocations()
    {
        return !_locations.isEmpty();
    }

    /** FSM Action */
    synchronized void updateExistingReplica()
    {
        assert !_locations.isEmpty();

        initiateCopy(new CellPath(_locations.remove(0)));

        /* Small optimisation to avoid having too many lists allocated.
         */
        if (_locations.isEmpty()) {
            _locations = Collections.emptyList();
        }
    }

    /** FSM Action */
    synchronized void initiateCopy()
    {
        try {
            initiateCopy(selectPool());
        } catch (NoSuchElementException e) {
            _target = null;
            _executor.execute(new LoggingTask(new Runnable() {
                    @Override
                    public void run() {
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
        FileAttributes fileAttributes = _entry.getFileAttributes();
        _target = target;
        _pool.send(_target,
                   new PoolMigrationCopyReplicaMessage(_uuid,
                                                       _source,
                                                       fileAttributes,
                                                       getTargetState(),
                                                       getTargetStickyRecords(),
                                                       _definition.computeChecksumOnUpdate,
                                                       _definition.forceSourceMode),
                   PoolMigrationCopyReplicaMessage.class,
                   new Callback("copy_"));
    }

    /** FSM Action */
    synchronized void cancelCopy()
    {
        _pool.send(_target,
                   new PoolMigrationCancelMessage(_uuid,
                                                  _source,
                                                  getPnfsId()),
                   PoolMigrationCancelMessage.class,
                   new Callback("cancel_"));
    }

    /** FSM Action */
    synchronized void movePin()
    {
        Collection<StickyRecord> records = new ArrayList();
        for (StickyRecord record: _entry.getStickyRecords()) {
            if (isPin(record)) {
                records.add(record);
            }
        }

        Callback<PinManagerMovePinMessage> callback = new Callback<>("move_");
        if (records.isEmpty()) {
            callback.success(null);
        } else {
            String target = _target.getDestinationAddress().getCellName();
            PinManagerMovePinMessage message =
                new PinManagerMovePinMessage(getPnfsId(), records, _source, target);
            _pinManager.send(message,
                             PinManagerMovePinMessage.class,
                             callback);
        }
    }

    /** FSM Action */
    void notifyCancelled()
    {
        _executor.execute(new LoggingTask(new Runnable() {
                @Override
                public void run()
                {
                    _job.taskCancelled(Task.this);
                }
            }));
    }

    /** FSM Action */
    void fail(final String message)
    {
        _executor.execute(new LoggingTask(new Runnable() {
                @Override
                public void run()
                {
                    _job.taskFailed(Task.this, message);
                }
            }));
    }

    /** FSM Action */
    void failPermanently(final String message)
    {
        _executor.execute(new LoggingTask(new Runnable() {
                @Override
                public void run()
                {
                    _job.taskFailedPermanently(Task.this, message);
                }
            }));
    }

    /** FSM Action */
    void notifyCompleted()
    {
        _executor.execute(new LoggingTask(new Runnable() {
                @Override
                public void run()
                {
                    _job.taskCompleted(Task.this);
                }
            }));
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
            _executor.schedule(new LoggingTask(task),
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
        _pool.send(_target,
                   new PoolMigrationPingMessage(_uuid,
                                                _source,
                                                getPnfsId()),
                   PoolMigrationPingMessage.class, new Callback("ping_"));
    }

    /**
     * Starts the task.
     */
    synchronized public void run()
    {
        _fsm.run();
    }

    /**
     * Cancels the task, if not already completed. This will trigger a
     * notification (postponed).
     */
    synchronized public void cancel()
    {
        _fsm.cancel();
    }

    /**
     * Helper class implementing the MessageCallback interface,
     * forwarding all messages as events to the state machine. Events
     * a forwarded via an executor to guarantee asynchronous delivery
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
            Class<?>[] parameterTypes = new Class[arguments.length];
            for (int i = 0; i < arguments.length; i++) {
                parameterTypes[i] = arguments[i].getClass();
            }
            final Method m =
                ReflectionUtils.resolve(_fsm.getClass(), _prefix + name,
                                        parameterTypes);
            if (m != null) {
                _executor.execute(new LoggingTask(new Runnable() {
                        @Override
                        public void run() {
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
                    }));
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
        public void timeout(CellPath path)
        {
            transition("timeout");
        }

        @Override
        public void noroute(CellPath path)
        {
            transition("noroute");
        }
    }

    protected class LoggingTask implements Runnable
    {
        private final Runnable _inner;

        public LoggingTask(Runnable r)
        {
            _inner = r;
        }

        @Override
        public void run()
        {
            try {
                _inner.run();
            } catch (RuntimeException | Error e) {
                Thread me = Thread.currentThread();
                me.getUncaughtExceptionHandler().uncaughtException(me, e);
                fail(e.getMessage());
            }
        }
    }
}
