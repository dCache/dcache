package org.dcache.services.billing.plots.util;

import java.util.Properties;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.plots.exceptions.TimeFrameFactoryInitializationException;
import org.dcache.services.billing.plots.exceptions.TimeFramePlotException;

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
    }

    void initialize(IBillingInfoAccess access)
                    throws TimeFrameFactoryInitializationException;

    void initialize(IBillingInfoAccess access, String propertiesPath)
                    throws TimeFrameFactoryInitializationException;

    void initialize(IBillingInfoAccess access, Properties properties)
                    throws TimeFrameFactoryInitializationException;

    /**
     * @param subtitles
     *            (titles for each section of the plot)
     */
    ITimeFramePlot createPlot(String plotName, String[] subtitles);

    /**
     * Histogram for DCache reads/writes (size).
     *
     * @param write
     *            (reads = false)
     */
    ITimeFrameHistogram createDcBytesHistogram(TimeFrame timeFrame,
                    boolean write) throws TimeFramePlotException;

    /**
     * Histogram for HSM system stage/store (size).
     *
     * @param write
     *            (true = store; false = stage)
     */
    ITimeFrameHistogram createHsmBytesHistogram(TimeFrame timeFrame,
                    boolean write) throws TimeFramePlotException ;

    /**
     * Histogram for DCache number of read/write operations.
     */
    ITimeFrameHistogram createDcTransfersHistogram(TimeFrame timeFrame,
                    boolean write) throws TimeFramePlotException ;

    /**
     * Histogram for HSM number of stage/store operations.
     *
     * @param write
     *            (true = store; false = stage)
     */
    ITimeFrameHistogram createHsmTransfersHistogram(TimeFrame timeFrame,
                    boolean write) throws TimeFramePlotException ;

    /**
     * Histograms for connection time on DCache operations.
     *
     * @return triple histogram array (minimum, maximum, average)
     */
    public ITimeFrameHistogram[] createDcConnectTimeHistograms(
                    TimeFrame timeFrame) throws TimeFramePlotException ;

    /**
     * Histogram for pool cost.
     */
    public ITimeFrameHistogram createCostHistogram(TimeFrame timeFrame)
                    throws TimeFramePlotException ;

    /**
     * Histogram for cache hits/misses.
     *
     * @return histogram pair (cached, notcached)
     */
    ITimeFrameHistogram[] createHitHistograms(TimeFrame timeFrame)
                    throws TimeFramePlotException;
}
