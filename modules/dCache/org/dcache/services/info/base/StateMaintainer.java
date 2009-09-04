package org.dcache.services.info.base;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

/**
 * The StateMaintainer class provides the machinery for processing
 * StateUpdate objects independently of whichever Thread created them. It is
 * also responsible for purging those metrics that have expired.
 */
public class StateMaintainer implements StateUpdateManager {

    private static final Logger _log = Logger.getLogger( StateMaintainer.class);

    /** Our scheduler */
    final private ScheduledExecutorService _scheduler;

    /** The number of pending requests, queued up in the scheduler */
    final private AtomicInteger _pendingRequestCount = new AtomicInteger();

    /** Our link to the business logic for update dCache state */
    private StateCaretaker _caretaker;

    /**
     * The Future for the next scheduled metric purge, or null if no such
     * activity has been scheduled
     */
    private ScheduledFuture<?> _metricExpiryFuture;
    private Date _metricExpiryDate;

    /**
     * Create a new StateMaintainer with a link to some StateCaretaker
     * 
     * @param caretaker
     *            the StateCaretaker that will undertake the business logic
     *            of updating dCache state.
     */
    public StateMaintainer( final StateCaretaker caretaker) {
        _caretaker = caretaker;
        _scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Alter which StateCaretaker the StateMaintainer will use.
     * 
     * @param caretaker
     */
    synchronized void setStateCaretaker( final StateCaretaker caretaker) {
        _caretaker = caretaker;
    }

    @Override
    public int countPendingUpdates() {
        return _pendingRequestCount.get();
    }

    @Override
    public synchronized void enqueueUpdate( final StateUpdate pendingUpdate) {
        _pendingRequestCount.incrementAndGet();
        _scheduler.submit( new Runnable() {
            @Override
            public void run() {
                _caretaker.processUpdate( pendingUpdate);
                _pendingRequestCount.decrementAndGet();
                checkScheduledExpungeActivity();
            }
        });
    }

    @Override
    public synchronized void shutdown() throws InterruptedException {
        List<Runnable> unprocessed = _scheduler.shutdownNow();
        if( !unprocessed.isEmpty())
            _log.info( "Shutting down with " + unprocessed.size() +
                       " pending updates");
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

        if( earliestMetricExpiry == null && _metricExpiryDate == null)
            return;

        if( _metricExpiryDate == null) {
            scheduleMetricExpunge( earliestMetricExpiry);
        } else if( !_metricExpiryDate.equals( earliestMetricExpiry)) {
            _metricExpiryFuture.cancel( false);
            scheduleMetricExpunge( earliestMetricExpiry);
        }
    }

    /**
     * Create a new scheduled task to expunge metrics. This method doesn't
     * cancel an existing scheduled task and will always schedule activity.
     * <p>
     * This method should be called only when we know the value from
     * {@link StateCaretaker#getEarliestMetricExpiryDate()} has changed.
     */
    synchronized void scheduleMetricExpunge() {
        scheduleMetricExpunge( _caretaker.getEarliestMetricExpiryDate());
    }

    /**
     * If whenExpunge is not null then schedule a task to call
     * {@link StateCaretaker#removeExpiredMetrics()} then call
     * {@link #scheduleMetricExpunge()} to schedule the next metric purge
     * activity. If whenExpunge is null then nothing happens.
     * 
     * @param whenExpunge
     *            some time in the future to schedule a task or null.
     */
    private void scheduleMetricExpunge( final Date whenExpunge) {
        _metricExpiryDate = whenExpunge;

        if( whenExpunge == null) {
            _metricExpiryFuture = null;
            return;
        }

        long delay = whenExpunge.getTime() - System.currentTimeMillis();

        _metricExpiryFuture = _scheduler.schedule( new Runnable() {
            public void run() {
                _caretaker.removeExpiredMetrics();
                scheduleMetricExpunge();
            }
        }, delay, TimeUnit.MILLISECONDS);
    }
}
