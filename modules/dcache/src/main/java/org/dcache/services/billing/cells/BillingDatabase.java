package org.dcache.services.billing.cells;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.dcache.cells.CellMessageReceiver;
import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.DoorRequestData;
import org.dcache.services.billing.db.data.MoverData;
import org.dcache.services.billing.db.data.PnfsBaseInfo;
import org.dcache.services.billing.db.data.PoolCostData;
import org.dcache.services.billing.db.data.PoolHitData;
import org.dcache.services.billing.db.data.StorageData;
import org.dcache.services.billing.db.exceptions.BillingInitializationException;
import org.dcache.services.billing.db.exceptions.BillingStorageException;
import org.dcache.services.billing.plots.util.ITimeFrameHistogram;
import org.dcache.services.billing.plots.util.ITimeFrameHistogramFactory;
import org.dcache.services.billing.plots.util.ITimeFramePlot;
import org.dcache.services.billing.plots.util.PlotGridPosition;
import org.dcache.services.billing.plots.util.TimeFrame;
import org.dcache.services.billing.plots.util.TimeFrame.BinType;
import org.dcache.services.billing.plots.util.TimeFrame.Type;
import org.dcache.services.billing.plots.util.TimeFramePlotFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import com.google.common.collect.ImmutableList;

import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.InfoMessage;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PoolCostInfoMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.StorageInfoMessage;

/**
 * This class is responsible for the processing of messages from other domains
 * regarding transfers and pool usage. It calls out to a IBillingInfoAccess
 * implementation to handle persistence of the data. It also optionally runs a
 * daemon to generate histogram images and write them to a well-known location.
 *
 * @see IBillingInfoAccess
 */
public final class BillingDatabase implements CellMessageReceiver, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BillingDatabase.class);

    /*
     * for histogram plotting
     */
    private static final List<String> TYPE = ImmutableList.of("bytes_rd",
                    "bytes_wr", "transfers_rd", "transfers_wr", "time", "hits",
                    "cost");
    private static final List<String> EXT = ImmutableList.of("_dy", "_wk",
                    "_mo", "_yr");
    private static final String DEFAULT_PLOT_REFRESH_IN_MINUTES = "30";
    private static final String DEFAULT_PROPERTIES
        = "org/dcache/services/billing/plot/plot.properties";

    private static List<String> TITLES;

    public static List<String> getEXT() {
        return EXT;
    }

    public static String getTITLE(int i) {
        if (TITLES == null) {
            return null;
        }
        return TITLES.get(i);
    }

    public static List<String> getTYPE() {
        return TYPE;
    }

    /*
     * for daily, weekly, monthly and yearly
     */
    private static TimeFrame[] generateTimeFrames() {
        final TimeFrame[] timeFrame = new TimeFrame[4];
        Calendar high = TimeFrame.computeHighTimeFromNow(BinType.HOUR);
        timeFrame[0] = getConfiguredTimeFrame(high, BinType.HOUR, Type.DAY);
        high = TimeFrame.computeHighTimeFromNow(BinType.DAY);
        timeFrame[1] = getConfiguredTimeFrame(high, BinType.DAY, Type.WEEK);
        timeFrame[2] = getConfiguredTimeFrame(high, BinType.DAY, Type.MONTH);
        timeFrame[3] = getConfiguredTimeFrame(high, BinType.DAY, Type.YEAR);
        return timeFrame;
    }

    /*
     * Configures the boundaries and interval.
     */
    private static TimeFrame getConfiguredTimeFrame(Calendar high, BinType bin,
                    Type type) {
        final TimeFrame timeFrame = new TimeFrame(high.getTimeInMillis());
        timeFrame.setTimebin(bin);
        timeFrame.setTimeframe(type);
        timeFrame.configure();
        return timeFrame;
    }

    private static String getTypeName(int type, int tFrame) {
        return TYPE.get(type) + EXT.get(tFrame);
    }

    private IBillingInfoAccess access;

    /*
     * for histogram generation: injected
     */
    private String propertiesPath;
    private String plotsDir;
    private String imgType;
    private boolean generatePlots = false;

    /*
     * for histogram generation: state
     */
    private ITimeFrameHistogramFactory factory;
    private List<String> timeDescription;
    private Long timeout;
    private File plotDirF;
    private Thread plotD;

    private boolean running = false;

    /**
     * Sets the properties for plotting, initializes factory, and starts run()
     * in a new Thread if {@link #generatePlots} is true.
     */
    public void initialize() throws Throwable {
        if (generatePlots) {
            logger.debug("initializing plotting ...");

            final Properties properties = new Properties();
            final ClassLoader classLoader
                = Thread.currentThread().getContextClassLoader();
            initializeProperties(classLoader, properties);
            synchronizePlotProperties(properties);

            plotDirF = new File(plotsDir);
            if (plotDirF.exists()) {
                if (!plotDirF.isDirectory()) {
                    throw new IOException(
                                    "plots directory is not a directory: "
                                                    + plotDirF);
                }
            }

            final TimeFramePlotFactory plotFactory
                = TimeFramePlotFactory.getInstance(access);
            final String impl
                = properties.getProperty(ITimeFramePlot.FACTORY_TYPE);
            factory = plotFactory.create(impl, properties);

            plotD = new Thread(this);
            plotD.start();
        }
    }

    public void messageArrived(InfoMessage info) {
        try {
            access.put(convert(info));
        } catch (final BillingStorageException e) {
            logger.error("Can't log billing via BillingInfoAccess: {}",
                            e.getMessage(), e);
            logger.info("Trying to reconnect");

            try {
                access.close();
                access.initialize();
                access.put(convert(info));
            } catch (final BillingInitializationException e1) {
                logger.error("Could not restart BillingInfoAccess: {}",
                                e1.getMessage());
            } catch (final BillingStorageException e2) {
                logger.error("Second attempt: still can't log via BillingInfoAccess: {}",
                                e2.getMessage(), e2);
            }
        }
    }

    /**
     * Sleeps for the given timeout, then regenerates the plots.
     */
    @Override
    public void run() {
        logger.debug("starting run ...");
        setRunning(true);

        final boolean[] isWrite = new boolean[] { false, true, false, true };
        final boolean[] isSize = new boolean[] { true, true, false, false };

        while (isRunning()) {
            try {
                logger.debug("generating time frames ...");
                final TimeFrame[] timeFrames = generateTimeFrames();

                logger.debug("generating plots ...");
                for (int tFrame = 0; tFrame < timeFrames.length; tFrame++) {
                    final Date low = timeFrames[tFrame].getLow();
                    int type = 0;
                    for (; type < 3; type++) {
                        generateReadWritePlot(getTypeName(type, tFrame),
                                        getTitle(type, tFrame, low),
                                        timeFrames[tFrame], isWrite[type],
                                        isSize[type]);
                    }
                    generateConnectionTimePlot(getTypeName(type, tFrame),
                                    getTitle(type, tFrame, low),
                                    timeFrames[tFrame]);
                    ++type;
                    generateHitsPlot(getTypeName(type, tFrame),
                                    getTitle(type, tFrame, low),
                                    timeFrames[tFrame]);
                    ++type;
                    generateCostPlot(getTypeName(type, tFrame),
                                    getTitle(type, tFrame, low),
                                    timeFrames[tFrame]);
                }
            } catch (final Throwable t) {
                logger.error("error generating billing plots; quitting ...", t);
                return;
            }

            try {
                logger.debug("sleeping ...");
                Thread.sleep(timeout);
            } catch (final InterruptedException ignored) {
            }
        }
    }

    @Required
    public void setAccess(IBillingInfoAccess access) {
        this.access = access;
    }

    public void setGeneratePlots(boolean generatePlots) {
        this.generatePlots = generatePlots;
    }

    public void setImgType(String imgType) {
        this.imgType = imgType;
    }

    public void setPlotsDir(String plotsDir) {
        this.plotsDir = plotsDir;
    }

    public void setPlotsTimeout(Integer timeoutInMins) {
        timeout = TimeUnit.MINUTES.toMillis(timeoutInMins);
    }

    public void setPropertiesPath(String propertiesPath) {
        this.propertiesPath = propertiesPath;
    }

    public synchronized void shutDown() {
        if (running) {
            running = false;
            plotD.interrupt();
            try {
                plotD.join();
            } catch (final InterruptedException ignored) {
            }
        }
        access.close();
    }

    /*
     * Converts from the InfoMessage type to the storage type.
     */
    private PnfsBaseInfo convert(InfoMessage info) {
        if (info instanceof MoverInfoMessage) {
            return new MoverData((MoverInfoMessage) info);
        }
        if (info instanceof DoorRequestInfoMessage) {
            return new DoorRequestData((DoorRequestInfoMessage) info);
        }
        if (info instanceof StorageInfoMessage) {
            return new StorageData((StorageInfoMessage) info);
        }
        if (info instanceof PoolCostInfoMessage) {
            return new PoolCostData((PoolCostInfoMessage) info);
        }
        if (info instanceof PoolHitInfoMessage) {
            return new PoolHitData((PoolHitInfoMessage) info);
        }
        return null;
    }

    /*
     * Auxiliary for histograms differentiating reads and writes.
     */
    private ITimeFrameHistogram[] createReadWriteHistograms(
                    TimeFrame timeFrame, boolean write, boolean size)
                    throws Throwable {
        if (size) {
            return new ITimeFrameHistogram[] {
                            factory.createDcBytesHistogram(timeFrame, write),
                            factory.createHsmBytesHistogram(timeFrame, write) };
        }
        return new ITimeFrameHistogram[] {
                        factory.createDcTransfersHistogram(timeFrame, write),
                        factory.createHsmTransfersHistogram(timeFrame, write) };
    }

    private void generateConnectionTimePlot(String fileName, String title,
                    TimeFrame timeFrame) {
        try {
            final ITimeFramePlot plot = factory.createPlot(fileName,
                            new String[] { title });
            final ITimeFrameHistogram[] histogram
                = factory.createDcConnectTimeHistograms(timeFrame);
            final PlotGridPosition pos = new PlotGridPosition(0, 0);
            for (final ITimeFrameHistogram h : histogram) {
                plot.addHistogram(pos, h);
            }
            plot.plot();
            logger.debug("generateConnectionTimePlot completed for " + fileName);
        } catch (final Throwable t) {
            logger.error("could not generate " + fileName + " for "
                            + timeFrame.getHigh(), t);
        }
    }

    private void generateCostPlot(String fileName, String title,
                    TimeFrame timeFrame) {
        try {
            final ITimeFramePlot plot = factory.createPlot(fileName,
                            new String[] { title });
            final ITimeFrameHistogram h = factory.createCostHistogram(timeFrame);
            final PlotGridPosition pos = new PlotGridPosition(0, 0);
            plot.addHistogram(pos, h);
            plot.plot();
            logger.debug("generateCostPlot completed for " + fileName);
        } catch (final Throwable t) {
            logger.error("could not generate " + fileName + " for "
                            + timeFrame.getHigh(), t);
        }
    }

    private void generateHitsPlot(String fileName, String title,
                    TimeFrame timeFrame) {
        try {
            final ITimeFramePlot plot = factory.createPlot(fileName,
                            new String[] { title });
            final ITimeFrameHistogram[] histogram
                = factory.createHitHistograms(timeFrame);
            final PlotGridPosition pos = new PlotGridPosition(0, 0);
            for (final ITimeFrameHistogram h : histogram) {
                plot.addHistogram(pos, h);
            }
            plot.plot();
            logger.debug("generateHitsPlot completed for " + fileName);
        } catch (final Throwable t) {
            logger.error("could not generate " + fileName + " for "
                            + timeFrame.getHigh(), t);
        }
    }

    private void generateReadWritePlot(String fileName, String title,
                    TimeFrame timeFrame, boolean write, boolean size) {
        try {
            final ITimeFramePlot plot = factory.createPlot(fileName,
                            new String[] { title });
            logger.debug("generateReadWritePlot created ITimeFramePlot for "
                            + fileName);
            final ITimeFrameHistogram[] histogram
                = createReadWriteHistograms(timeFrame, write, size);
            logger.debug("generateReadWritePlot created histogram set for "
                            + fileName);
            final PlotGridPosition pos = new PlotGridPosition(0, 0);
            for (final ITimeFrameHistogram h : histogram) {
                plot.addHistogram(pos, h);
            }
            plot.plot();
            logger.debug("generateReadWritePlot completed for " + fileName);
        } catch (final Throwable t) {
            logger.error("could not generate " + fileName + " for "
                            + timeFrame.getHigh(), t);
        }
    }

    private String getTitle(int type, int tFrame, Date low) {
        return TITLES.get(type) + " (" + timeDescription.get(tFrame) + low
                        + ")";
    }

    private void initializeProperties(ClassLoader classLoader,
                    Properties properties) throws IOException {
        logger.debug("initializeProperties ...");
        if (propertiesPath != null && !"".equals(propertiesPath.trim())) {
            final File file = new File(propertiesPath);
            if (!file.exists()) {
                throw new FileNotFoundException(
                                "Cannot run plotting thread for properties file: "
                                                + file);
            }
            final InputStream stream = new FileInputStream(file);
            try {
                properties.load(stream);
            } finally {
                stream.close();
            }
        } else {
            final URL resource = classLoader.getResource(DEFAULT_PROPERTIES);
            if (resource == null) {
                throw new FileNotFoundException("Cannot run plotting thread"
                                + "; cannot find resource "
                                + DEFAULT_PROPERTIES);
            }
            properties.load(resource.openStream());
        }

        TITLES = ImmutableList.of(
                        properties.getProperty(ITimeFramePlot.TITLE_BYTES_RD),
                        properties.getProperty(ITimeFramePlot.TITLE_BYTES_WR),
                        properties.getProperty(ITimeFramePlot.TITLE_TRANSF_RD),
                        properties.getProperty(ITimeFramePlot.TITLE_TRANSF_WR),
                        properties.getProperty(ITimeFramePlot.TITLE_CONN_TM),
                        properties.getProperty(ITimeFramePlot.TITLE_CACHE_HITS),
                        properties.getProperty(ITimeFramePlot.TITLE_POOL_COST));

        timeDescription = ImmutableList.of(
                        properties.getProperty(ITimeFramePlot.DESC_DAILY),
                        properties.getProperty(ITimeFramePlot.DESC_WEEKLY),
                        properties.getProperty(ITimeFramePlot.DESC_MONTHLY),
                        properties.getProperty(ITimeFramePlot.DESC_YEARLY));
    }

    private synchronized boolean isRunning() {
        return running;
    }

    private synchronized void setRunning(boolean running) {
        this.running = running;
    }

    /*
     * makes sure overwritten properties (if any) are correctly propagated and
     * that internal properties correspond to those in the definition file if
     * the former are undefined.
     */
    private void synchronizePlotProperties(Properties properties) {
        logger.debug("synchronizePlotProperties ...");
        if (plotsDir != null) {
            properties.setProperty(ITimeFramePlot.EXPORT_SUBDIR, plotsDir);
        } else {
            plotsDir = properties.getProperty(ITimeFramePlot.EXPORT_SUBDIR);
        }

        if (plotsDir == null) {
            throw new IllegalArgumentException("plots directory is undefined");
        }

        if (imgType != null) {
            properties.setProperty(ITimeFramePlot.EXPORT_TYPE, imgType);
        } else {
            imgType = properties.getProperty(ITimeFramePlot.EXPORT_TYPE);
        }

        if (imgType == null) {
            throw new IllegalArgumentException("image type is undefined");
        }

        properties.setProperty(ITimeFramePlot.EXPORT_EXTENSION, "." + imgType);

        if (timeout == null) {
            final String value
                = properties.getProperty(ITimeFramePlot.THREAD_TIMEOUT_TYPE,
                                         DEFAULT_PLOT_REFRESH_IN_MINUTES);
            timeout = TimeUnit.MINUTES.toMillis(Long.parseLong(value));
        }
    }
}
