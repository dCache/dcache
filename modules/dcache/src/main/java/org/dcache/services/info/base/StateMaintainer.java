package org.dcache.services.info.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.util.Date;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;

import org.dcache.cells.CellMessageSender;
import org.dcache.commons.util.NDC;
import org.dcache.util.FireAndForgetTask;

/**
 * The StateMaintainer class provides the machinery for processing
 * StateUpdate objects independently of whichever Thread created them. It is
 * also responsible for purging those metrics that have expired.
 */
public class StateMaintainer implements StateUpdateManager, CellMessageSender {

    private static final Logger _log = LoggerFactory.getLogger( StateMaintainer.class);

    private static final boolean CANCEL_RUNNING_METRIC_EXPUNGE = false;

    /** Our scheduler */
    private ScheduledExecutorService _scheduler;

    /** The number of pending requests, queued up in the scheduler */
    final private AtomicInteger _pendingRequestCount = new AtomicInteger();

    /** Our link to the business logic for update dCache state */
    private volatile StateCaretaker _caretaker;

    private String _cellName = "unknown";
    private String _domainName = "unknown";

    /**
     * The Future for the next scheduled metric purge, or null if no such
     * activity has been scheduled.  Access and updates to this object are
     * protected by the this (StateMaintainer) object monitor.
     */
    private ScheduledFuture<?> _metricExpiryFuture;
    private Date _metricExpiryDate;

    @Required
    public void setCaretaker(StateCaretaker caretaker)
    {
        _caretaker = caretaker;
    }

    @Required
    public void setExecutor(ScheduledExecutorService executor)
    {
        _scheduler = executor;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        CellInfo info = endpoint.getCellInfo();
        _domainName = info.getDomainName();
        _cellName = info.getCellName();
    }

    /**
     * Alter which StateCaretaker the StateMaintainer will use.
     *
     * @param caretaker
     */
    void setStateCaretaker( final StateCaretaker caretaker) {
        _caretaker = caretaker;
    }

    @Override
    public int countPendingUpdates() {
        return _pendingRequestCount.get();
    }

    @Override
    public void enqueueUpdate( final StateUpdate pendingUpdate) {
        if( _log.isDebugEnabled()) {
            _log.debug("enqueing job to process update " + pendingUpdate);
        }

        final NDC ndc = NDC.cloneNdc();

        _pendingRequestCount.incrementAndGet();
        _scheduler.execute( new FireAndForgetTask( new Runnable() {
            @Override
            public void run() {
                CDC.reset(_cellName, _domainName);
                NDC.set(ndc);
                try {
                    if( _log.isDebugEnabled()) {
                        _log.debug("starting job to process update " + pendingUpdate);
                    }

                    _caretaker.processUpdate( pendingUpdate);
                    checkScheduledExpungeActivity();

                    if( _log.isDebugEnabled()) {
                        _log.debug("finished job to process update " + pendingUpdate);
                    }
                } finally {
                    _pendingRequestCount.decrementAndGet();
                    CDC.clear();
                }
            }
        }));
    }

    @Override
    public void shutdown()
    {
        List<Runnable> unprocessed = _scheduler.shutdownNow();
        if( !unprocessed.isEmpty()) {
            _log.info("Shutting down with " + unprocessed.size() +
                    " pending updates");
        } else {
            _log.debug("Shutting down without any pending updates");
        }
    }

    /**
     * Check StateCaretaker to obtain the earliest time when a metric will
     * expire. If this value has changed then reschedule the metric expiry
     * activity.
     * <p>
     * It is safe to call this method whenever the value of
     * {@link StateCaretaker#getEarliestMetricExpiryDate()} could possible
     * have changed.
     */
    synchronized void checkScheduledExpungeActivity() {
        Date earliestMetricExpiry = _caretaker.getEarliestMetricExpiryDate();

        if( earliestMetricExpiry == null && _metricExpiryDate == null) {
            return;
        }

        // If the metric expiry date has changed, we try to cancel the update.
        if( _metricExpiryDate != null && !_metricExpiryDate.equals( earliestMetricExpiry)) {
            if( _log.isDebugEnabled()) {
                Date now = new Date();
                long delay = _metricExpiryDate.getTime() - now.getTime();
                _log.debug( "Cancelling existing metric purge, due to take place in " + delay / 1000.0 + " s");
            }

            /*  If the cancel fails (returns false) then the metric expunge is
             *  currently being processed.  When this completes, a new metric
             *  expiry job will be scheduled automatically, so we don't need to
             *  do anything.
             */
            if( _metricExpiryFuture.cancel( CANCEL_RUNNING_METRIC_EXPUNGE)) {
                _metricExpiryDate = null;
            }
        }

        if( _metricExpiryDate == null) {
            scheduleMetricExpunge(earliestMetricExpiry);
        }
    }

    /**
     * If whenExpunge is not null then schedule a task to call
     * {@link StateCaretaker#removeExpiredMetrics()} then call
     * {@link #scheduleScheduleMetricExpunge()} to schedule the next metric
     * purge activity. If whenExpunge is null then no action is taken.
     *
     * @param whenExpunge
     *            some time in the future to schedule a task or null.
     */
    private synchronized void scheduleMetricExpunge( final Date whenExpunge) {
        _metricExpiryDate = whenExpunge;

        if( whenExpunge == null) {
            _metricExpiryFuture = null;
            return;
        }

        long delay = whenExpunge.getTime() - System.currentTimeMillis();

        if( _log.isDebugEnabled()) {
            _log.debug("Scheduling next metric purge in " + delay / 1000.0 + " s");
        }

        try {
            _metricExpiryFuture = _scheduler.schedule( new FireAndForgetTask( new Runnable() {
                @Override
                public void run() {
                    _log.debug( "Starting metric purge");
                    _caretaker.removeExpiredMetrics();
                    scheduleMetricExpunge();
                    _log.debug( "Metric purge completed");
                }
            }), delay, TimeUnit.MILLISECONDS);
        } catch( RejectedExecutionException e) {
            _log.debug( "Failed to enqueue expunge task as queue is not accepting further work.");
        }
    }

    /**
     * Create a new scheduled task to expunge metrics.  If the existing metric
     * expunge job is not finished then nothing is done.  If it is finished
     * then a new task is submitted.
     * <p>
     * This method should only be called when we know the value from
     * {@link StateCaretaker#getEarliestMetricExpiryDate()} has changed to
     * avoid creating competing tasks.
     */
    synchronized protected void scheduleMetricExpunge() {
        scheduleMetricExpunge( _caretaker.getEarliestMetricExpiryDate());
    }
}
