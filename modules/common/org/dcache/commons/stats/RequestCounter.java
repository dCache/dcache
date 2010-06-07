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
    public int getFailed();

    /**
     *
     * @return name of this counter
     */
    public String getName();

    /**
     *
     * @return number of request invocations known to this counter
     */
    public int getTotalRequests();

    /**
     * Reset the counter.
     */
    public void reset();

    /**
     * Shutdown the counter.
     */
    public void shutdown();

}
