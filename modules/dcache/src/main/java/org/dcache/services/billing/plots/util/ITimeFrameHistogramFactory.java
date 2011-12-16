package org.dcache.services.billing.plots.util;

import java.util.Properties;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.plots.exceptions.TimeFrameFactoryInitializationException;

/**
 * Defines factory interface for generating implementation-specific
 * {@link ITimeFrameHistogram} instances. Needs to be initialized with an
 * instance of {@link IBillingInfoAccess}.
 *
 * @see ITimeFrameHistogram
 * @see IBillingInfoAccess
 *
 * @author arossi
 */
public interface ITimeFrameHistogramFactory {

    final double GB = 1024 * 1024 * 1024;

    enum AggregateType {
        MIN, MAX, AVG
    };

    /**
     * @param access
     */
    void initialize(IBillingInfoAccess access)
                    throws TimeFrameFactoryInitializationException;

    /**
     * @param access
     * @param propertiesPath
     * @throws TimeFrameFactoryInitializationException
     */
    void initialize(IBillingInfoAccess access, String propertiesPath)
                    throws TimeFrameFactoryInitializationException;

    /**
     * @param access
     * @param properties
     * @throws TimeFrameFactoryInitializationException
     */
    void initialize(IBillingInfoAccess access, Properties properties)
                    throws TimeFrameFactoryInitializationException;

    /**
     * @param plotName
     * @param subtitles
     *            (titles for each section of the plot)
     * @return plot
     */
    ITimeFramePlot createPlot(String plotName, String[] subtitles);

    /**
     * Histogram for DCache reads/writes (size).
     *
     * @param timeFrame
     * @param write
     *            (reads = false)
     * @return histogram
     */
    ITimeFrameHistogram createDcBytesHistogram(TimeFrame timeFrame,
                    boolean write) throws Throwable;

    /**
     * Histogram for HSM system stage/store (size).
     *
     * @param timeFrame
     * @param write
     *            (true = store; false = stage)
     * @return histogram
     */
    ITimeFrameHistogram createHsmBytesHistogram(TimeFrame timeFrame,
                    boolean write) throws Throwable;

    /**
     * Histogram for DCache number of read/write operations.
     *
     * @param timeFrame
     * @param write
     * @return histogram
     */
    ITimeFrameHistogram createDcTransfersHistogram(TimeFrame timeFrame,
                    boolean write) throws Throwable;

    /**
     * Histogram for HSM number of stage/store operations.
     *
     * @param timeFrame
     * @param write
     *            (true = store; false = stage)
     * @return histogram
     */
    ITimeFrameHistogram createHsmTransfersHistogram(TimeFrame timeFrame,
                    boolean write) throws Throwable;

    /**
     * Histograms for connection time on DCache operations.
     *
     * @param timeFrame
     * @return triple histogram array (minimum, maximum, average)
     */
    public ITimeFrameHistogram[] createDcConnectTimeHistograms(
                    TimeFrame timeFrame) throws Throwable;

    /**
     * Histogram for pool cost.
     *
     * @param timeFrame
     * @return histogram
     */
    public ITimeFrameHistogram createCostHistogram(TimeFrame timeFrame)
                    throws Throwable;

    /**
     * Histogram for cache hits/misses.
     *
     * @param timeFrame
     * @return histogram pair (cached, notcached)
     */
    ITimeFrameHistogram[] createHitHistograms(TimeFrame timeFrame)
                    throws Throwable;
}
