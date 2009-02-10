package org.dcache.pool.migration;

import java.util.TimerTask;
import java.util.Timer;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.PrintWriter;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.services.pinmanager1.PinManagerMovePinMessage;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;

import org.apache.log4j.Logger;

public class Task
{
    private final static Logger _log = Logger.getLogger(Job.class);
    private final static AtomicInteger _counter = new AtomicInteger();

    private final TaskContext _fsm;

    private final Job _job;

    private final CellStub _pool;
    private final CellStub _pnfs;
    private final CellStub _pinManager;
    private final ScheduledExecutorService _executor;
    private final String _source;
    private final String _pinPrefix;
    private final boolean _mustMovePins;

    private final long _id;

    private final CacheEntry _entry;

    private final CacheEntryMode _targetMode;

    private ScheduledFuture _timerTask;
    private List<String> _locations = Collections.emptyList();
    private CellPath _target;

    public Task(Job job,
                CellStub pool,
                CellStub pnfs,
                CellStub pinManager,
                ScheduledExecutorService executor,
                String source,
                CacheEntry entry,
                CacheEntryMode targetMode,
                boolean mustMovePins)
    {
        _id = _counter.getAndIncrement();
        _fsm = new TaskContext(this);
        _job = job;
        _pool = pool;
        _pnfs = pnfs;
        _pinManager = pinManager;
        _executor = executor;
        _source = source;
        _entry = entry;
        _targetMode = targetMode;
        _mustMovePins = mustMovePins;
        _pinPrefix =
            _pinManager.getDestinationPath().getDestinationAddress().getCellName();
    }

    public boolean getMustMovePins()
    {
        return _mustMovePins;
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
        return _pool.getTimeout() * 2;
    }

    /**
     * Time in milliseconds before we fail the task if we loose the
     * cell connection.
     */
    public long getNoResponseTimeout()
    {
        return _pool.getTimeout() * 2;
    }

    /**
     * Time in milliseconds before we fail the task if we do not get
     * CopyFinished.
     */
    public long getTaskDeadTimeout()
    {
        return _pool.getTimeout();
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
        switch (_targetMode.state) {
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
        if (_targetMode.state == CacheEntryMode.State.SAME) {
            for (StickyRecord record: _entry.getStickyRecords()) {
                if (!isPin(record)) {
                    result.add(record);
                }
            }
        }
        result.addAll(_targetMode.stickyRecords);
        return result;
    }

    /** Adds status information about the task to <code>pw</code>. */
    synchronized void getInfo(PrintWriter pw)
    {
        if (_target != null) {
            pw.println(String.format("[%d] %s: %s -> %s",
                                     _id,
                                     _entry.getPnfsId(),
                                     _fsm.getState(),
                                     _target));
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
        if (message.getTaskId() == _id) {
            _fsm.messageArrived(message);
        }
    }

    /** FSM Action */
    synchronized void queryLocations()
    {
        _pnfs.send(new PnfsGetCacheLocationsMessage(getPnfsId()),
                   PnfsGetCacheLocationsMessage.class,
                   new Callback<PnfsGetCacheLocationsMessage>()
                   {
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
    synchronized void setLocations(List<String> locations)
    {
        _locations = new ArrayList(locations);
        _locations.retainAll(_job.getPools());
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
        long ttl = System.currentTimeMillis() + (_pool.getTimeout() / 2);
        Message message =
            new PoolMigrationUpdateReplicaMessage(getPnfsId(),
                                                  getTargetState(),
                                                  getTargetStickyRecords(),
                                                  ttl);
        _target = new CellPath(_locations.remove(0));
        _pool.send(_target, message,
                   PoolMigrationUpdateReplicaMessage.class,
                   new Callback());

        /* Small optimisation to avoid having too many lists allocated. */
        if (_locations.isEmpty()) {
            _locations = Collections.emptyList();
        }
    }

    /** FSM Action */
    synchronized void initiateCopy()
    {
        try {
            PnfsId pnfsId = _entry.getPnfsId();
            StorageInfo storageInfo = _entry.getStorageInfo();

            _target = _job.selectPool();
            _pool.send(_target,
                       new PoolMigrationCopyReplicaMessage(_source,
                                                           pnfsId,
                                                           _id,
                                                           storageInfo,
                                                           getTargetState(),
                                                           getTargetStickyRecords()),
                       PoolMigrationCopyReplicaMessage.class,
                       new Callback());
        } catch (NoSuchElementException e) {
            _target = null;
            _executor.execute(new LoggingTask(new Runnable() {
                    public void run() {
                        synchronized (Task.this) {
                            _fsm.nopools();
                        }
                    }
                }));
        }
    }

    /** FSM Action */
    synchronized void cancelCopy()
    {
        _pool.send(_target,
                   new PoolMigrationCancelMessage(_source, getPnfsId(), _id),
                   PoolMigrationCancelMessage.class,
                   new Callback());
    }

    /** FSM Action */
    synchronized void movePin()
    {
        Collection<StickyRecord> records = new ArrayList();
        for (StickyRecord record: _entry.getStickyRecords()) {
            String owner = record.owner();
            if (isPin(record)) {
                records.add(record);
            }
        }

        Callback callback = new Callback();
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
                public void run()
                {
                    _job.taskCancelled(Task.this);
                }
            }));
    }

    /** FSM Action */
    void notifyFailed()
    {
        _executor.execute(new LoggingTask(new Runnable() {
                public void run()
                {
                    _job.taskFailed(Task.this);
                }
            }));
    }

    /** FSM Action */
    void notifyCompleted()
    {
        _executor.execute(new LoggingTask(new Runnable() {
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
                   new PoolMigrationPingMessage(_source, getPnfsId(), _id),
                   PoolMigrationPingMessage.class, new Callback());
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
    class Callback<T extends Message> implements MessageCallback<T>
    {
        public void success(T message)
        {
            _executor.execute(new LoggingTask(new Runnable() {
                    public void run() {
                        synchronized (Task.this) {
                            _fsm.success();
                        }
                    }
                }));
        }

        public void failure(final int rc, final Object cause)
        {
            _executor.execute(new LoggingTask(new Runnable() {
                    public void run() {
                        synchronized (Task.this) {
                            _fsm.failure(rc);
                        }
                    }
                }));
        }

        public void timeout()
        {
            _executor.execute(new LoggingTask(new Runnable() {
                    public void run() {
                        synchronized (Task.this) {
                            _fsm.timeout();
                        }
                    }
                }));
        }

        public void noroute()
        {
            _executor.execute(new LoggingTask(new Runnable() {
                    public void run() {
                        synchronized (Task.this) {
                            _fsm.noroute();
                        }
                    }
                }));
        }
    }

    protected class LoggingTask implements Runnable
    {
        private final Runnable _inner;

        public LoggingTask(Runnable r)
        {
            _inner = r;
        }

        public void run()
        {
            try {
                _inner.run();
            } catch (Exception e) {
                _log.error(e, e);
            }
        }
    }
}