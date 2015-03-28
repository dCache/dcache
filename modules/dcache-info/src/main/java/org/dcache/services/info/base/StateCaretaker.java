package org.dcache.services.info.base;

import java.util.Date;

/**
 * A Class that implements StateCaretaker implements the business logic for
 * updating dCache state. This includes applying a StateUpdate object,
 * querying to discover when mortal metrics should next be removed and to
 * remove those metrics that have expired.
 * <p>
 * It is expected that the Thread calling these methods will block until the
 * work is completed.
 */
public interface StateCaretaker {

    /**
     * Process an StateUpdate object. The calling thread will block until the
     * process has completed.
     */
    void processUpdate(StateUpdate update);

    /**
     * Discover the earliest that a mortal metric is to be removed. Calling
     * {@link #removeExpiredMetrics()} prior to that Date will have no
     * effect. null is returned if there are no expiring metrics.
     *
     * @return the Date when the earliest metric that should be removed or
     *         null if there are none.
     */
    Date getEarliestMetricExpiryDate();

    /**
     * Remove any expired metrics. The calling thread will block until the
     * process has completed.
     */
    void removeExpiredMetrics();
}
