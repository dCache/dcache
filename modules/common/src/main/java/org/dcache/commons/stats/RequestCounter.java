/*
 * Request Counter interface
 */

package org.dcache.commons.stats;

/**
 *
 * @author timur
 */
public interface RequestCounter {

    /**
     *
     * @return number of faild request invocations known to this counter
     */
    int getFailed();

    /**
     *
     * @return name of this counter
     */
    String getName();

    /**
     *
     * @return number of request invocations known to this counter
     */
    int getTotalRequests();

    /**
     * Reset the counter.
     */
    void reset();

    /**
     * Shutdown the counter.
     */
    void shutdown();

    int getSuccessful();
}
