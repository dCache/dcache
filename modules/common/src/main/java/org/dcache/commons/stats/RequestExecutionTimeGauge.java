package org.dcache.commons.stats;


public interface RequestExecutionTimeGauge {

    /**
     * return average over the last period, and start new period
     * @return
     */
    double resetAndGetAverageExecutionTime();

    /**
     * return average over the lifetime of the gauge
     * @return
     */
    double getAverageExecutionTime();

    /**
     * @return the RMS of executionTime
     */
    double getExecutionTimeRMS();

    /**
     * @return the lastExecutionTime
     */
    long getLastExecutionTime();

    /**
     * @return the maxExecutionTime
     */
    long getMaxExecutionTime();

    /**
     * @return the minExecutionTime
     */
    long getMinExecutionTime();

    /**
     * @return the name
     */
    String getName();

    double getStandardDeviation();

    /**
     *
     * @return standard error of the mean
     */
    double getStandardError();

    /**
     * @return the startTime
     */
    long getStartTime();

    /**
     * @return the updateNum
     */
    long getUpdateNum();

    /**
     *
     * @param nextExecTime
     */
    void update(long nextExecTime);

    /**
     * Reset the gauge.
     */
    void reset();
}
