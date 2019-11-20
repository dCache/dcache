package org.dcache.pool.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import dmg.cells.nucleus.DelayedReply;

import org.dcache.pool.repository.AbstractStateChangeListener;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryChangeEvent;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StickyChangeEvent;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.FireAndForgetTask;
import org.dcache.util.expression.Expression;

import static com.google.common.base.Preconditions.checkState;

import dmg.cells.nucleus.CellMessage;

/**
 * Encapsulates a job as defined by a user command.
 *
 * A job is a collection of tasks, each task transferring a single
 * replica. The tasks are scheduled by the job. Whereas a job defines
 * a bulk operation, a task encapsulates a transfer of a single
 * replica.
 *
 * Jobs monitor the local repository for changes. If a replica changes
 * state before it is transferred, and the replica no longer passes the
 * selection criteria of the job, then it will not be transferred. If
 * it is in the process of being transferred, then the transfer is
 * cancelled. If the transfer has already completed, then nothing
 * happens.
 *
 * Jobs can be defined as permanent. A permanent job will monitor the
 * repository for state changes. Should a replica be added or change
 * state in such a way that is passes the selection criteria of the
 * job, then it is added to the transfer queue of the job. A permanent
 * job does not terminate, even if its transfer queue becomes
 * empty. Permanent jobs are saved to the pool setup file and restored
 * on pool start.
 *
 * Jobs can be in any of the following states:
 *
 * NEW            Job has not been started yet
 * INITIALIZING   Initial scan of repository
 * RUNNING        Job runs (schedules new tasks)
 * SLEEPING       A task failed; no tasks are scheduled for 10 seconds
 * PAUSED         Pause expression evaluates to true; no tasks are
 *                scheduled for 10 seconds
 * STOPPING       Stop expression evaluate to true; waiting or tasks
 *                to stop
 * SUSPENDED      Job suspended by user; no tasks are scheduled
 * CANCELLING     Job cancelled by user; waiting for tasks to stop
 * CANCELLED      Job cancelled by user; no tasks are running
 * FINISHED       Job completed
 * FAILED         Job failed
 */
public class Job
    extends AbstractStateChangeListener implements TaskCompletionHandler
{
    enum State { NEW, INITIALIZING, RUNNING, SLEEPING, PAUSED, SUSPENDED,
            STOPPING, CANCELLING, CANCELLED, FINISHED, FAILED }

    private static final Logger _log = LoggerFactory.getLogger(Job.class);

    private final Set<PnfsId> _queued = new LinkedHashSet<>();
    private final Map<PnfsId,Long> _sizes = new HashMap<>();
    private final Map<PnfsId,Task> _running = new HashMap<>();
    private final BlockingQueue<Error> _errors = new ArrayBlockingQueue<>(15);
    private final Map<PoolMigrationJobCancelMessage,DelayedReply> _cancelRequests =
        new HashMap<>();

    private final JobStatistics _statistics = new JobStatistics();
    private final MigrationContext _context;
    private final JobDefinition _definition;
    private final TaskParameters _taskParameters;
    private final String _pinPrefix;

    private final Lock _lock = new ReentrantLock(true);

    private volatile State _state;
    private int _concurrency;

    private Future<?> _refreshTask;

    public Job(MigrationContext context, JobDefinition definition)
    {
        _context = context;
        _definition = definition;
        _concurrency = 1;
        _state = State.NEW;

        _taskParameters = new TaskParameters(context.getPoolStub(), context.getPnfsStub(), context.getPinManagerStub(),
                                             context.getExecutor(), definition.selectionStrategy,
                                             definition.poolList, definition.isEager, definition.isMetaOnly,
                                             definition.computeChecksumOnUpdate, definition.forceSourceMode,
                                             definition.maintainAtime, definition.replicas);

        _pinPrefix = context.getPinManagerStub().getDestinationPath().getDestinationAddress().getCellName();
    }

    public void start()
    {
        _lock.lock();
        try {
            checkState(_state == State.NEW);
            _state = State.INITIALIZING;

            long refreshPeriod = _definition.refreshPeriod;
            ScheduledExecutorService executor = _context.getExecutor();

            _refreshTask =
                    executor.scheduleWithFixedDelay(new FireAndForgetTask(() -> {
                        _definition.sourceList.refresh();
                        _definition.poolList.refresh();
                    }), 0, refreshPeriod, TimeUnit.MILLISECONDS);

            executor.submit(new FireAndForgetTask(() -> {
                try {
                    _context.getRepository().addListener(Job.this);
                    populate();

                    _lock.lock();
                    try {
                        if (getState() == State.INITIALIZING) {
                            setState(State.RUNNING);
                        }
                    } finally {
                        _lock.unlock();
                    }
                } catch (InterruptedException e) {
                    _log.error("Migration job was interrupted");
                } finally {
                    _lock.lock();
                    try {
                        switch (getState()) {
                        case INITIALIZING:
                            setState(State.FAILED);
                            break;
                        case CANCELLING:
                            schedule();
                            break;
                        }
                    } finally {
                        _lock.unlock();
                    }
                }
            }));
        } finally {
            _lock.unlock();
        }
    }

    public JobDefinition getDefinition()
    {
        return _definition;
    }

    public int getConcurrency()
    {
        _lock.lock();
        try {
            return _concurrency;
        } finally {
            _lock.unlock();
        }
    }

    public void setConcurrency(int concurrency)
    {
        _lock.lock();
        try {
            _concurrency = concurrency;
            schedule();
        } finally {
            _lock.unlock();
        }
    }

    public void addError(Error error)
    {
        _lock.lock();
        try {
            while (!_errors.offer(error)) {
                _errors.poll();
            }
        } finally {
            _lock.unlock();
        }
    }

    /** Adds status information about the job to <code>pw</code>. */
    public void getInfo(PrintWriter pw)
    {
        _lock.lock();
        try {
            long total = _statistics.getTotal();
            long completed = _statistics.getTransferred();
            pw.println("State      : " + _state);
            pw.println("Queued     : " + _queued.size());
            pw.println("Attempts   : " + _statistics.getAttempts());
            pw.println("Targets    : " + _definition.poolList);

            if (total > 0) {
                switch (getState()) {
                case NEW:
                    break;
                case RUNNING:
                case SUSPENDED:
                case CANCELLING:
                case STOPPING:
                case SLEEPING:
                case PAUSED:
                    pw.println("Completed  : "
                               + _statistics.getCompleted() + " files; "
                               + _statistics.getTransferred() + " bytes; "
                               + (100 * completed / total) + "%");
                    pw.println("Total      : " + total + " bytes");
                    break;

                case INITIALIZING:
                case FINISHED:
                    pw.println("Completed  : "
                               + _statistics.getCompleted() + " files; "
                               + _statistics.getTransferred() + " bytes");
                    pw.println("Total      : " + total + " bytes");
                    break;

                case CANCELLED:
                case FAILED:
                    pw.println("Completed  : "
                               + _statistics.getCompleted() + " files; "
                               + _statistics.getTransferred() + " bytes");
                    break;
                }
            }

            pw.println("Concurrency: " + _concurrency);
            pw.println("Running tasks:");
            List<Task> tasks = new ArrayList<>(_running.values());
            Collections.sort(tasks, (t1, t2) -> Long.compare(t1.getId(), t2.getId()));
            for (Task task : tasks) {
                task.getInfo(pw);
            }

            if (!_errors.isEmpty()) {
                pw.println("Most recent errors:");
                for (Error error : _errors) {
                    pw.println(error);
                }
            }
        } finally {
            _lock.unlock();
        }
    }

    /**
     * Scans the repository for files and adds corresponding tasks to
     * the job.
     */
    private void populate()
        throws InterruptedException
    {
        try {
            Repository repository = _context.getRepository();
            Iterable<PnfsId> files = repository;

            if (_definition.comparator != null) {
                List<PnfsId> all = new ArrayList<>();
                for (PnfsId pnfsId: files) {
                    all.add(pnfsId);
                }
                Comparator<PnfsId> order =
                    new CacheEntryOrder(repository, _definition.comparator);
                Collections.sort(all, order);
                files = all;
            }

            for (PnfsId pnfsId: files) {
                try {
                    _lock.lock();
                    try {
                        if (_state != State.INITIALIZING) {
                            break;
                        }
                        CacheEntry entry = repository.getEntry(pnfsId);
                        if (accept(entry)) {
                            add(entry);
                        }
                    } finally {
                        _lock.unlock();
                    }
                } catch (FileNotInCacheException e) {
                    // File was removed before we got to it - not a
                    // problem.
                } catch (CacheException e) {
                    _log.error("Failed to load entry: {}", e.getMessage());
                }
            }
        } catch (IllegalStateException e) {
            // This means the repository was not initialized yet. Not
            // a big problem, since we will be notified about each
            // entry during initialization.
        }
    }

    /**
     * Cancels a job. All running tasks are cancelled.
     */
    public void cancel(boolean force, String why)
    {
        _lock.lock();
        try {
            if (_state == State.CANCELLED ||
                _state == State.FAILED ||
                _state == State.FINISHED) {
                for (Map.Entry<PoolMigrationJobCancelMessage,DelayedReply> entry: _cancelRequests.entrySet()) {
                    entry.getValue().reply(entry.getKey());
                }
            } else  if (_state != State.INITIALIZING && _running.isEmpty()) {
                setState(State.CANCELLED);
            } else {
                setState(State.CANCELLING);
                if (force) {
                    for (Task task : _running.values()) {
                        task.cancel(why);
                    }
                }
            }
        } finally {
            _lock.unlock();
        }
    }

    /**
     * Similar to cancel(false), but the job will eventually end up in
     * FINISHED rather than CANCELLED.
     */
    @GuardedBy("_lock")
    private void stop()
    {
        if (_state != State.RUNNING && _state != State.SUSPENDED &&
            _state != State.STOPPING && _state != State.SLEEPING &&
            _state != State.PAUSED) {
            throw new IllegalStateException("The job cannot be stopped in its present state");
        }
        if (_running.isEmpty()) {
            setState(State.FINISHED);
        } else {
            setState(State.STOPPING);
        }
    }

    /**
     * Pauses a job. Pause is similar to suspend, but it will
     * periodically reevaluate the pause predicate and automatically
     * resume the job when the predicate evaluates to false.
     */
    @GuardedBy("_lock")
    private void pause()
    {
        if (_state != State.RUNNING && _state != State.SUSPENDED &&
            _state != State.SLEEPING && _state != State.PAUSED) {
            throw new IllegalStateException("The job cannot be stopped in its present state");
        }
        setState(State.PAUSED);
    }

    /**
     * Suspends a job. No new tasks are scheduled.
     */
    public void suspend()
    {
        _lock.lock();
        try {
            if (_state != State.RUNNING && _state != State.SUSPENDED &&
                _state != State.SLEEPING && _state != State.PAUSED) {
                throw new IllegalStateException("Cannot suspend a job that does not run");
            }
            setState(State.SUSPENDED);
        } finally {
            _lock.unlock();
        }
    }

    /**
     * Resumes a previously suspended task.
     */
    public void resume()
    {
        _lock.lock();
        try {
            if (_state != State.SUSPENDED) {
                throw new IllegalStateException("Cannot resume a job that does not run");
            }
            setState(State.RUNNING);
        } finally {
            _lock.unlock();
        }
    }

    /** Returns the current state of the job. */
    public State getState()
    {
        return _state;
    }

    /**
     * Sets the state of the job.
     *
     * Closely coupled to the <code>schedule</code> method.
     *
     * @see schedule
     */
    private void setState(State state)
    {
        _lock.lock();
        try {
            if (_state != state) {
                _state = state;
                switch (_state) {
                case RUNNING:
                    schedule();
                    break;

                case SLEEPING:
                    _context.getExecutor().schedule(new FireAndForgetTask(() -> {
                        _lock.lock();
                        try {
                            if (getState() == State.SLEEPING) {
                                setState(State.RUNNING);
                            }
                        } finally {
                            _lock.unlock();
                        }
                    }), 10, TimeUnit.SECONDS);
                    break;

                case PAUSED:
                    _context.getExecutor().schedule(new FireAndForgetTask(() -> {
                        _lock.lock();
                        try {
                            if (getState() == State.PAUSED) {
                                Expression stopWhen = _definition.stopWhen;
                                if (stopWhen != null && evaluateLifetimePredicate(stopWhen)) {
                                    stop();
                                }
                                Expression pauseWhen = _definition.pauseWhen;
                                if (!evaluateLifetimePredicate(pauseWhen)) {
                                    setState(State.RUNNING);
                                }
                            }
                        } finally {
                            _lock.unlock();
                        }
                    }), 10, TimeUnit.SECONDS);
                    break;

                case FINISHED:
                case CANCELLED:
                case FAILED:
                    _queued.clear();
                    _sizes.clear();
                    _context.getRepository().removeListener(this);
                    _refreshTask.cancel(false);

                    for (Map.Entry<PoolMigrationJobCancelMessage,DelayedReply> entry: _cancelRequests.entrySet()) {
                        entry.getValue().reply(entry.getKey());
                    }
                    _cancelRequests.clear();
                    break;
                }
            }
        } finally {
            _lock.unlock();
        }
    }

    /**
     * Schedules jobs, depending on the current state and available
     * resources.
     *
     * Closely coupled to the <code>setState</code> method.
     *
     * @see setState
     */
    @GuardedBy("_lock")
    private void schedule()
    {
        if (_state == State.CANCELLING && _running.isEmpty()) {
            setState(State.CANCELLED);
        } else if (_state != State.INITIALIZING && _state != State.NEW && !_definition.isPermanent
                   && _queued.isEmpty() && _running.isEmpty()) {
            setState(State.FINISHED);
        } else if (_state == State.STOPPING && _running.isEmpty()) {
            setState(State.FINISHED);
        } else if (_state == State.RUNNING &&
                   (!_definition.sourceList.isValid() ||
                    !_definition.poolList.isValid())) {
            setState(State.SLEEPING);
        } else if (_state == State.RUNNING) {
            Iterator<PnfsId> i = _queued.iterator();
            while ((_running.size() < _concurrency) && i.hasNext()) {
                Expression stopWhen = _definition.stopWhen;
                if (stopWhen != null && evaluateLifetimePredicate(stopWhen)) {
                    stop();
                    break;
                }
                Expression pauseWhen = _definition.pauseWhen;
                if (pauseWhen != null && evaluateLifetimePredicate(pauseWhen)) {
                    pause();
                    break;
                }

                PnfsId pnfsId = i.next();
                if (!_context.lock(pnfsId)) {
                    addError(new Error(0, pnfsId, "File is locked"));
                    continue;
                }

                try {
                    i.remove();
                    Repository repository = _context.getRepository();
                    CacheEntry entry = repository.getEntry(pnfsId);

                    Task task = new Task(_taskParameters, this, _context.getPoolName(),
                                         entry.getPnfsId(),
                                         getTargetState(entry), getTargetStickyRecords(entry),
                                         getPins(entry), entry.getFileAttributes(), entry.getLastAccessTime());
                    _running.put(pnfsId, task);
                    _statistics.addAttempt();
                    task.run();
                } catch (FileNotInCacheException e) {
                    _sizes.remove(pnfsId);
                } catch (CacheException e) {
                    _log.error("Migration job failed to read entry: {}", e.getMessage());
                    setState(State.FAILED);
                    break;
                } catch (InterruptedException e) {
                    _log.error("Migration job was interrupted: {}", e.getMessage());
                    setState(State.FAILED);
                    break;
                } finally {
                    if (!_running.containsKey(pnfsId)) {
                        _context.unlock(pnfsId);
                    }
                }
            }

            if (_running.isEmpty()) {
                if (!_definition.isPermanent && _queued.isEmpty()) {
                    setState(State.FINISHED);
                } else {
                    setState(State.SLEEPING);
                }
            }
        }
    }

    private ReplicaState getTargetState(CacheEntry entry)
    {
        switch (_definition.targetMode.state) {
        case SAME:
            return entry.getState();
        case CACHED:
            return ReplicaState.CACHED;
        case PRECIOUS:
            return ReplicaState.PRECIOUS;
        default:
            throw new IllegalStateException("Unsupported target mode");
        }
    }

    private List<StickyRecord> getPins(CacheEntry entry)
    {
        if (!_definition.mustMovePins) {
            return Collections.emptyList();
        }

        List<StickyRecord> pins = new ArrayList<>();
        for (StickyRecord record: entry.getStickyRecords()) {
            if (isPin(record)) {
                pins.add(record);
            }
        }
        return pins;
    }

    private List<StickyRecord> getTargetStickyRecords(CacheEntry entry)
    {
        List<StickyRecord> result = new ArrayList<>();
        if (_definition.targetMode.state == CacheEntryMode.State.SAME) {
            for (StickyRecord record: entry.getStickyRecords()) {
                if (!isPin(record)) {
                    result.add(record);
                }
            }
        }
        result.addAll(_definition.targetMode.stickyRecords);
        return result;
    }

    /**
     * Returns true if and only if <code>entry</code> is accepted by
     * all filters.
     */
    private boolean accept(CacheEntry entry)
    {
        return _definition.filter.test(entry);
    }

    /** Adds a new task to the job. */
    @GuardedBy("_lock")
    private void add(CacheEntry entry)
    {
        PnfsId pnfsId = entry.getPnfsId();
        if (!_queued.contains(pnfsId) && !_running.containsKey(pnfsId)) {
            long size = entry.getReplicaSize();
            _queued.add(pnfsId);
            _sizes.put(pnfsId, size);
            _statistics.addToTotal(size);
            schedule();
        }
    }

    /** Removes a task from the job. */
    @GuardedBy("_lock")
    private void remove(PnfsId pnfsId, String why)
    {
        Task task = _running.get(pnfsId);
        if (task != null) {
            task.cancel(why);
        } else if (_queued.remove(pnfsId)) {
            _sizes.remove(pnfsId);
        }
    }

    /** Callback from repository. */
    @Override
    public void stateChanged(StateChangeEvent event)
    {
        PnfsId pnfsId = event.getPnfsId();
        if (event.getNewState() == ReplicaState.REMOVED) {
            _lock.lock();
            try {
                remove(pnfsId, "replica deleted");
            } finally {
                _lock.unlock();
            }
        } else {
            // We don't call entryChanged because during repository
            // initialization stateChanged is called and we want to
            // add the file to the job even if the state didn't change.
            CacheEntry entry = event.getNewEntry();
            if (!accept(entry)) {
                _lock.lock();
                try {
                    if (!_running.containsKey(pnfsId)) {
                        remove(pnfsId, "file now " + event.getNewState() + ", so no longer matches criteria");
                    }
                } finally {
                    _lock.unlock();
                }
            } else if (_definition.isPermanent) {
                _lock.lock();
                try {
                    add(entry);
                } finally {
                    _lock.unlock();
                }
            }
        }
    }

    @Override
    public void accessTimeChanged(EntryChangeEvent event)
    {
        entryChanged(event);
    }

    @Override
    public void stickyChanged(StickyChangeEvent event)
    {
        entryChanged(event);
    }

    private void entryChanged(EntryChangeEvent event)
    {
        PnfsId pnfsId = event.getPnfsId();
        CacheEntry entry = event.getNewEntry();
        if (!accept(entry)) {
            _lock.lock();
            try {
                if (!_running.containsKey(pnfsId)) {
                    String type = event instanceof StickyChangeEvent ? "sticky" : "atime";
                    remove(pnfsId, type + " changed, so file no longer matches criteria");
                }
            } finally {
                _lock.unlock();
            }
        } else if (_definition.isPermanent && !accept(event.getOldEntry())) {
            _lock.lock();
            try {
                add(entry);
            } finally {
                _lock.unlock();
            }
        }
    }

    /** Callback from task: Task is dead, remove it. */
    @Override
    public void taskCancelled(Task task)
    {
        _lock.lock();
        try {
            PnfsId pnfsId = task.getPnfsId();
            _running.remove(pnfsId);
            _sizes.remove(pnfsId);
            _context.unlock(pnfsId);
            schedule();
        } finally {
            _lock.unlock();
        }
    }

    /** Callback from task: Task failed, reschedule it. */
    @Override
    public void taskFailed(Task task, int rc, String msg)
    {
        _lock.lock();
        try {
            PnfsId pnfsId = task.getPnfsId();
            if (task == _running.remove(pnfsId)) {
                _queued.add(pnfsId);
                _context.unlock(pnfsId);
            }

            if (_state == State.RUNNING) {
                setState(State.SLEEPING);
            } else {
                schedule();
            }

            addError(new Error(task.getId(), pnfsId, msg));
        } finally {
            _lock.unlock();
        }
    }

    /** Callback from task: Task failed permanently, remove it. */
    @Override
    public void taskFailedPermanently(Task task, int rc, String msg)
    {
        _lock.lock();
        try {
            PnfsId pnfsId = task.getPnfsId();
            _running.remove(pnfsId);
            _sizes.remove(pnfsId);
            _context.unlock(pnfsId);
            schedule();

            addError(new Error(task.getId(), pnfsId, msg));
        } finally {
            _lock.unlock();
        }
    }

    /** Callback from task: Task is done, remove it. */
    @Override
    public void taskCompleted(Task task)
    {
        PnfsId pnfsId = task.getPnfsId();
        applySourceMode(pnfsId);
        _lock.lock();
        try {
            _running.remove(pnfsId);
            _context.unlock(pnfsId);
            _statistics.addCompleted(_sizes.remove(pnfsId));
            schedule();
        } finally {
            _lock.unlock();
        }
    }

    public Object messageArrived(CellMessage envelope, PoolMigrationJobCancelMessage message)
    {
        DelayedReply reply = new DelayedReply();
        _lock.lock();
        try {
            _cancelRequests.put(message, reply);
            StringBuilder why = new StringBuilder();
            why.append("as requested by ").append(envelope.getSourceAddress());
            String reason = message.getReason();
            // The null tests is for backwards compatibility when back-porting.
            // It may be dropped in master branch.
            if (reason != null) {
                why.append(": ").append(reason);
            }
            cancel(message.isForced(), why.toString());
        } finally {
            _lock.unlock();
        }
        return reply;
    }

    /** Message handler. Delegates to proper task .*/
    public void messageArrived(PoolMigrationCopyFinishedMessage message)
    {
        Task task;
        _lock.lock();
        try {
            task = _running.get(message.getPnfsId());
        } finally {
            _lock.unlock();
        }
        if (task != null) {
            task.messageArrived(message);
        }
    }

    /** Apply sticky flags to file. */
    private void applySticky(PnfsId pnfsId, List<StickyRecord> records)
        throws CacheException, InterruptedException
    {
        for (StickyRecord record: records) {
            _context.getRepository().setSticky(pnfsId,
                                               record.owner(),
                                               record.expire(),
                                               true);
        }
    }

    /**
     * Returns true if and only if <code>records</code> contains an
     * entry for <code>owner</code>.
     */
    private boolean containsOwner(List<StickyRecord> records, String owner)
    {
        for (StickyRecord r: records) {
            if (r.owner().equals(owner)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if and only if <code>record</code> is owned by the
     * pin manager.
     */
    private boolean isPin(StickyRecord record)
    {
        return record.owner().startsWith(_pinPrefix);
    }

    /**
     * Returns true if and only if the given entry has any sticky
     * records owned by the pin manager.
     */
    private boolean isPinned(CacheEntry entry)
    {
        for (StickyRecord record: entry.getStickyRecords()) {
            if (isPin(record)) {
                return true;
            }
        }
        return false;
    }

    /** Apply source mode update to replica. */
    private void applySourceMode(PnfsId pnfsId)
    {
        try {
            CacheEntryMode mode = _definition.sourceMode;
            Repository repository = _context.getRepository();
            CacheEntry entry = repository.getEntry(pnfsId);
            switch (mode.state) {
            case SAME:
                applySticky(pnfsId, mode.stickyRecords);
                break;
            case DELETE:
                if (!isPinned(entry)) {
                    repository.setState(pnfsId, ReplicaState.REMOVED,
                            "migration job deleting source");
                    break;
                }
                // Fall through
            case REMOVABLE:
                List<StickyRecord> list = mode.stickyRecords;
                applySticky(pnfsId, list);
                for (StickyRecord record: entry.getStickyRecords()) {
                    String owner = record.owner();
                    if (!isPin(record) && !containsOwner(list, owner)) {
                        repository.setSticky(pnfsId, owner, 0, true);
                    }
                }
                repository.setState(pnfsId, ReplicaState.CACHED,
                        "migration job making source removable");
                break;
            case CACHED:
                applySticky(pnfsId, mode.stickyRecords);
                repository.setState(pnfsId, ReplicaState.CACHED,
                        "migration job making source cached");
                break;
            case PRECIOUS:
                repository.setState(pnfsId, ReplicaState.PRECIOUS,
                        "migration job making source precious");
                applySticky(pnfsId, mode.stickyRecords);
                break;
            }
        } catch (FileNotInCacheException e) {
            // File got remove before we could update it. TODO: log it
        } catch (IllegalTransitionException e) {
            // File is likely about to be removed. TODO: log it
        } catch (CacheException e) {
            _log.error("Migration job failed to update source mode: {}", e.getMessage());
            setState(State.FAILED);
        } catch (InterruptedException e) {
            _log.error("Migration job was interrupted");
            setState(State.FAILED);
        }
    }

    public boolean evaluateLifetimePredicate(Expression expression)
    {
        List<PoolManagerPoolInformation> sourceInformation =
            _definition.sourceList.getPools();
        if (sourceInformation.isEmpty()) {
            throw new RuntimeException("Bug detected: Source pool information was unavailable");
        }

        SymbolTable symbols = new SymbolTable();
        symbols.put(MigrationModule.CONSTANT_SOURCE,
                    sourceInformation.get(0));
        symbols.put(MigrationModule.CONSTANT_QUEUE_FILES,
                    _queued.size());
        symbols.put(MigrationModule.CONSTANT_QUEUE_BYTES,
                    _statistics.getTotal() - _statistics.getCompleted());
        symbols.put(MigrationModule.CONSTANT_TARGETS,
                    _definition.poolList.getPools().size());
        return expression.evaluateBoolean(symbols);
    }

    protected static class Error
    {
        private final long _id;
        private final long _time;
        private final PnfsId _pnfsId;
        private final String _error;

        public Error(long id, PnfsId pnfsId, String error)
        {
            _id = id;
            _time = System.currentTimeMillis();
            _pnfsId = pnfsId;
            _error = error;
        }

        public String toString()
        {
            return String.format("%tT [%d] %s: %s",
                                 _time, _id, _pnfsId, _error);
        }
    }
}
