package org.dcache.pool.migration;

import org.dcache.pool.repository.AbstractStateChangeListener;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.StickyRecord;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellPath;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Comparator;
import java.util.Collections;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates a job as defined by a user command.
 *
 * A job is a collection of tasks, each task transfering a single
 * replica. The tasks are scheduled by the job. Whereas a job defines
 * a bulk operation, a task encapsulates a transfer of a single
 * replica.
 *
 * Jobs monitor the local repository for changes. If a replica changes
 * state before it is transfered, and the replica no longer passes the
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
 * INITIALIZING   Initial scan of repository
 * RUNNING        Job runs (schedules new tasks)
 * SLEEPING       A task failed; no tasks are scheduled for 10 seconds
 * SUSPENDED      Job suspended by user; no tasks are scheduled
 * CANCELLING     Job cancelled by user; waiting for tasks to stop
 * CANCELLED      Job cancelled by user; no tasks are running
 * FINISHED       Job completed
 * FAILED         Job failed
 */
public class Job
    extends AbstractStateChangeListener
{
    enum State { INITIALIZING, RUNNING, SLEEPING, SUSPENDED,
            CANCELLING, CANCELLED, FINISHED, FAILED }

    private final static Logger _log = LoggerFactory.getLogger(Job.class);

    private final Set<PnfsId> _queued = new LinkedHashSet();
    private final Map<PnfsId,Long> _sizes = new HashMap();
    private final Map<PnfsId,Task> _running = new HashMap();
    private final Future _refreshTask;
    private final BlockingQueue<Error> _errors = new ArrayBlockingQueue(15);

    private final JobStatistics _statistics = new JobStatistics();
    private final ModuleConfiguration _configuration;
    private final JobDefinition _definition;
    private State _state;
    private int _concurrency;

    public Job(ModuleConfiguration configuration,
               JobDefinition definition)
    {
        ScheduledExecutorService executor = configuration.getExecutor();
        long refreshPeriod = definition.refreshPeriod;

        _configuration = configuration;
        _definition = definition;
        _concurrency = 1;
        _state = State.INITIALIZING;

        _refreshTask =
            executor.scheduleWithFixedDelay(new LoggingTask(new Runnable() {
                    public void run()
                    {
                        _definition.poolList.refresh();
                    }
                }), 0, refreshPeriod, TimeUnit.MILLISECONDS);

        executor.submit(new LoggingTask(new Runnable() {
                public void run()
                {
                    State state = State.FAILED;
                    try {
                        _configuration.getRepository().addListener(Job.this);
                        populate();
                        state = State.RUNNING;
                    } finally {
                        setState(state);
                    }
                }
            }));
    }

    public synchronized JobDefinition getDefinition()
    {
        return _definition;
    }

    public synchronized int getConcurrency()
    {
        return _concurrency;
    }

    public synchronized void setConcurrency(int concurrency)
    {
        _concurrency = concurrency;
        schedule();
    }

    /** Returns the total amount of data covered by this job. */
    public synchronized long getTotal()
    {
        long total = _statistics.getTransferred();
        for (long size: _sizes.values()) {
            total += size;
        }
        return total;
    }

    /** Adds status information about the job to <code>pw</code>. */
    public synchronized void getInfo(PrintWriter pw)
    {
        long total = getTotal();
        long completed = _statistics.getTransferred();
        pw.println("State      : " + _state);
        pw.println("Queued     : " + _queued.size());
        pw.println("Attempts   : " + _statistics.getAttempts());
        pw.println("Targets    : " + _definition.poolList);

        if (total > 0) {
            switch (getState()) {
            case RUNNING:
            case SUSPENDED:
            case CANCELLING:
                pw.println("Completed  : "
                           + _statistics.getCompleted() + " files; "
                           + _statistics.getTransferred() + " bytes; "
                           + ((int) 100 * completed / total) + "%");
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
        ArrayList<Task> tasks = new ArrayList(_running.values());
        Collections.sort(tasks, new Comparator<Task>() {
                public int compare(Task t1, Task t2)
                {
                    return (int)Math.signum(t1.getId() - t2.getId());
                }
            });
        for (Task task: tasks) {
            task.getInfo(pw);
        }

        if (!_errors.isEmpty()) {
            pw.println("Most recent errors:");
            for (Error error: _errors) {
                pw.println(error);
            }
        }
    }

    /**
     * Scans the repository for files and adds corresponding tasks to
     * the job.
     */
    private void populate()
    {
        try {
            Repository repository = _configuration.getRepository();
            Iterable<PnfsId> files = repository;

            if (_definition.comparator != null) {
                List<PnfsId> all = new ArrayList<PnfsId>();
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
                    synchronized (this) {
                        CacheEntry entry = repository.getEntry(pnfsId);
                        if (accept(entry)) {
                            add(entry);
                        }
                    }
                } catch (FileNotInCacheException e) {
                    // File was removed before we got to it - not a
                    // problem.
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
    public synchronized void cancel(boolean force)
    {
        if (_state != State.RUNNING && _state != State.SUSPENDED
            && _state != State.CANCELLING && _state != State.SLEEPING) {
            throw new IllegalStateException("The job cannot be cancelled in its present state");
        }
        if (_running.size() == 0) {
            setState(State.CANCELLED);
        } else {
            setState(State.CANCELLING);
            if (force) {
                for (Task task: _running.values()) {
                    task.cancel();
                }
            }
        }
    }

    /**
     * Suspends a job. No new tasks are scheduled.
     */
    public synchronized void suspend()
    {
        if (_state != State.RUNNING && _state != State.SLEEPING) {
            throw new IllegalStateException("Cannot suspend a job that does not run");
        }
        setState(State.SUSPENDED);
    }

    /**
     * Resumes a previously suspended task.
     */
    public synchronized void resume()
    {
        if (_state != State.SUSPENDED) {
            throw new IllegalStateException("Cannot resume a job that does not run");
        }
        setState(State.RUNNING);
    }

    /** Returns the current state of the job. */
    public synchronized State getState()
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
    private synchronized void setState(State state)
    {
        if (_state != state) {
            _state = state;
            switch (_state) {
            case RUNNING:
                schedule();
                break;

            case SLEEPING:
                _configuration.getExecutor().schedule(new LoggingTask(new Runnable() {
                        public void run()
                        {
                            synchronized (Job.this) {
                                if (getState() == State.SLEEPING) {
                                    setState(State.RUNNING);
                                }
                            }
                        }
                    }), 10, TimeUnit.SECONDS);
                break;

            case FINISHED:
            case CANCELLED:
            case FAILED:
                _queued.clear();
                _sizes.clear();
                _configuration.getRepository().removeListener(this);
                _refreshTask.cancel(false);
                break;
            }
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
    private synchronized void schedule()
    {
        if (_state == State.CANCELLING && _running.isEmpty()) {
            setState(State.CANCELLED);
        } else if (_state != State.INITIALIZING && !_definition.isPermanent
                   && _queued.isEmpty() && _running.isEmpty()) {
            setState(State.FINISHED);
        } else if (_state == State.RUNNING) {
            Iterator<PnfsId> i = _queued.iterator();
            while ((_running.size() < _concurrency) && i.hasNext()) {
                PnfsId pnfsId = i.next();
                try {
                    i.remove();
                    Repository repository = _configuration.getRepository();
                    CacheEntry entry = repository.getEntry(pnfsId);
                    Task task = new Task(this,
                                         _configuration.getPoolStub(),
                                         _configuration.getPnfsStub(),
                                         _configuration.getPinManagerStub(),
                                         _configuration.getExecutor(),
                                         _configuration.getPoolName(),
                                         entry,
                                         _definition);
                    _running.put(pnfsId, task);
                    _statistics.addAttempt();
                    task.run();
                } catch (FileNotInCacheException e) {
                    _sizes.remove(pnfsId);
                    continue;
                }
            }

            if (!_definition.isPermanent
                && _queued.isEmpty() && _running.isEmpty()) {
                setState(State.FINISHED);
            }
        }
    }

    /**
     * Returns true if and only if <code>entry</code> is accepted by
     * all filters.
     */
    private boolean accept(CacheEntry entry)
    {
        for (CacheEntryFilter filter: _definition.filters) {
            if (!filter.accept(entry)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if and only if the job contains a running task for
     * the given PNFS ID.
     */
    public synchronized boolean isRunning(PnfsId pnfsId)
    {
        return _running.containsKey(pnfsId);
    }

    /** Adds a new task to the job. */
    private synchronized void add(CacheEntry entry)
    {
        PnfsId pnfsId = entry.getPnfsId();
        if (!_queued.contains(pnfsId) && !_running.containsKey(pnfsId)) {
            long size = entry.getReplicaSize();
            _queued.add(pnfsId);
            _sizes.put(pnfsId, size);
            schedule();
        }
    }

    /** Removes a task from the job. */
    private synchronized void remove(PnfsId pnfsId)
    {
        Task task = _running.get(pnfsId);
        if (task != null) {
            task.cancel();
        } else if (_queued.remove(pnfsId)) {
            _sizes.remove(pnfsId);
        }
    }

    /** Callback from repository. */
    @Override
    public void stateChanged(StateChangeEvent event)
    {
        Repository repository = _configuration.getRepository();
        PnfsId pnfsId = event.getPnfsId();
        if (event.getNewState() == EntryState.REMOVED) {
            remove(pnfsId);
        } else {
            try {
                CacheEntry entry = repository.getEntry(pnfsId);
                if (!accept(entry)) {
                    synchronized (this) {
                        if (!_running.containsKey(pnfsId)) {
                            remove(pnfsId);
                        }
                    }
                } else if (_definition.isPermanent) {
                    add(entry);
                }
            } catch (FileNotInCacheException e) {
                remove(pnfsId);
            }
        }
    }

    /** Callback from task: Task is dead, remove it. */
    synchronized void taskCancelled(Task task)
    {
        PnfsId pnfsId = task.getPnfsId();
        _running.remove(pnfsId);
        _sizes.remove(pnfsId);
        schedule();
    }

    /** Callback from task: Task failed, reschedule it. */
    synchronized void taskFailed(Task task, String msg)
    {
        PnfsId pnfsId = task.getPnfsId();
        if (task == _running.remove(pnfsId)) {
            _queued.add(pnfsId);
        }

        if (_state == State.RUNNING) {
            setState(State.SLEEPING);
        } else {
            schedule();
        }

        Error error = new Error(task.getId(), pnfsId, msg);
        while (!_errors.offer(error)) {
            _errors.poll();
        }
    }

    /** Callback from task: Task failed permanently, remove it. */
    synchronized void taskFailedPermanently(Task task, String msg)
    {
        PnfsId pnfsId = task.getPnfsId();
        _running.remove(pnfsId);
        _sizes.remove(pnfsId);
        schedule();

        Error error = new Error(task.getId(), pnfsId, msg);
        while (!_errors.offer(error)) {
            _errors.poll();
        }
    }

    /** Callback from task: Task is done, remove it. */
    synchronized void taskCompleted(Task task)
    {
        PnfsId pnfsId = task.getPnfsId();
        applySourceMode(pnfsId);
        _running.remove(pnfsId);
        _statistics.addCompleted(_sizes.remove(pnfsId));
        schedule();
    }

    /** Message handler. Delegates to proper task .*/
    public void
        messageArrived(PoolMigrationCopyFinishedMessage message)
    {
        Task task;
        synchronized (this) {
            task = _running.get(message.getPnfsId());
        }
        if (task != null) {
            task.messageArrived(message);
        }
    }


    /** Apply sticky flags to file. */
    private void applySticky(PnfsId pnfsId, List<StickyRecord> records)
        throws FileNotInCacheException
    {
        for (StickyRecord record: records) {
            _configuration.getRepository().setSticky(pnfsId,
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
    private synchronized boolean isPin(StickyRecord record)
    {
        String prefix = _configuration.getPinManagerStub()
            .getDestinationPath().getDestinationAddress().getCellName();
        return record.owner().startsWith(prefix);
    }

    /**
     * Returns true if and only if the given entry has any sticky
     * records owned by the pin manager.
     */
    private synchronized boolean isPinned(CacheEntry entry)
    {
        for (StickyRecord record: entry.getStickyRecords()) {
            if (isPin(record)) {
                return true;
            }
        }
        return false;
    }

    /** Apply source mode update to replica. */
    private synchronized void applySourceMode(PnfsId pnfsId)
    {
        try {
            CacheEntryMode mode = _definition.sourceMode;
            Repository repository = _configuration.getRepository();
            CacheEntry entry = repository.getEntry(pnfsId);
            switch (mode.state) {
            case SAME:
                applySticky(pnfsId, mode.stickyRecords);
                break;
            case DELETE:
                if (!isPinned(entry)) {
                    repository.setState(pnfsId, EntryState.REMOVED);
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
                repository.setState(pnfsId, EntryState.CACHED);
                break;
            case CACHED:
                applySticky(pnfsId, mode.stickyRecords);
                repository.setState(pnfsId, EntryState.CACHED);
                break;
            case PRECIOUS:
                repository.setState(pnfsId, EntryState.PRECIOUS);
                applySticky(pnfsId, mode.stickyRecords);
                break;
            }
        } catch (FileNotInCacheException e) {
            // File got remove before we could update it. TODO: log it
        } catch (IllegalTransitionException e) {
            // File is likely about to be removed. TODO: log it
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
                _log.error(e.toString(), e);
            }
        }
    }

    protected class Error
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