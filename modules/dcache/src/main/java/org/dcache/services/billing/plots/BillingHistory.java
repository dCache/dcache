package org.dcache.services.billing.plots;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.exceptions.BillingInitializationException;
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
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

import dmg.util.Args;

/**
 * Generates historgrams (plot images) using an {@link IBillingAccessInfo}
 * implementation.
 *
 * @author arossi
 */
public class BillingHistory implements Runnable {

    /**
     * Adapted from {@link UniversalSpringCellApplicationContext}.<br>
     * <br>
     *
     * Makes command-line arguments available to Spring bean configuration.
     */
    private class BillingHistoryApplicationContext extends
    ClassPathXmlApplicationContext {

        private BillingHistoryApplicationContext() {
            super(args.getOption(CONTEXT_LOCATION));
        }

        private ByteArrayResource getArgumentsResource() {
            Args args = new Args(getArgs());
            args.shift();

            Properties contextProperties = new Properties();
            String arguments = args.toString().replaceAll("-\\$\\{[0-9]+\\}",
                            "");
            contextProperties.setProperty("arguments", arguments);
            for (Map.Entry<String, ?> e : args.optionsAsMap().entrySet()) {
                String key = e.getKey();
                Object value = e.getValue();
                contextProperties.setProperty(key, value.toString());
            }

            /*
             * Convert to byte array form such that we can make it available as
             * a Spring resource.
             */
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                contextProperties.store(out, "");
            } catch (IOException e) {
                /*
                 * This should never happen with a ByteArrayOutputStream.
                 */
                throw new RuntimeException("Unexpected exception", e);
            }
            final byte[] _domainContext = out.toByteArray();

            return new ByteArrayResource(_domainContext) {
                /**
                 * Fake file name to make PropertyPlaceholderConfigurer happy.
                 */
                @Override
                public String getFilename() {
                    return "arguments.properties";
                }
            };
        }

        @Override
        public Resource getResource(String location) {
            if (location.startsWith("arguments:")) {
                return getArgumentsResource();
            } else {
                return super.getResource(location);
            }
        }
    }

    public static final String[] TYPE = { "bytes_rd", "bytes_wr",
        "transfers_rd", "transfers_wr", "time", "hits", "cost" };
    public static final String[] EXT = { "_dy", "_wk", "_mo", "_yr" };

    protected static final String DEFAULT_SLEEP = "30";
    protected static final String DEFAULT_PROPERTIES = "org/dcache/services/billing/plot/plot.properties";
    protected static final String ACCESS_BEAN = "jdbc-billing-info-access";

    /**
     * DCache properties passed in on command line
     */
    protected static final String CONTEXT_LOCATION = "dbAccessContext";
    protected static final String PLOT_PROPERTIES_OPT = "plotsProperties";
    protected static final String PLOT_DIR_OPT = "plotsDir";
    protected static final String SUB_DIR_OPT = "subDir";
    protected static final String EXPORT_TYPE_OPT = "exportType";
    protected static final String EXPORT_EXT_OPT = "exportExt";
    protected static final String TIMEOUT_OPT = "plotsTimeout";

    public static String[] TITLE;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final TimeFrame[] timeFrame;
    protected final Properties properties;
    protected final Args args;

    protected String[] TIME_DESC;
    protected String plotDir;
    protected String subDir;
    protected String imgExt;
    protected String imgType;
    protected Long timeout;
    protected IBillingInfoAccess access;
    protected ITimeFrameHistogramFactory factory;
    protected boolean running;
    protected File plotDirF;

    /**
     * Actual constructor used when deployed to dcache.
     *
     * @param name
     * @param args
     */
    public BillingHistory(Args args) {
        this.args = args;
        properties = new Properties();
        timeFrame = new TimeFrame[4];
    }

    /**
     * Sleeps for the given timeout, then regenerates the plots.
     */
    @Override
    public void run() {
        logger.debug("starting run ...");
        try {
            initialize();
        } catch (Throwable t) {
            logger.error("error intializing billing history thread; quitting ...",
                            t);
            return;
        }

        if (plotDirF.exists()) {
            if (!plotDirF.isDirectory())
                plotDirF.delete();
        }

        setRunning(true);

        while (isRunning()) {
            try {
                if (!plotDirF.exists()) {
                    plotDirF.mkdirs();
                }

                logger.debug("generating time frames ...");
                generateTimeFrames();

                logger.debug("generating plots ...");
                for (int t = 0; t < timeFrame.length; t++) {
                    Date d = timeFrame[t].getLow();
                    generateReadWritePlot(TYPE[0] + EXT[t], TITLE[0] + " ("
                                    + TIME_DESC[t] + d + ")", timeFrame[t],
                                    false, true);
                    generateReadWritePlot(TYPE[1] + EXT[t], TITLE[1] + " ("
                                    + TIME_DESC[t] + d + ")", timeFrame[t],
                                    true, true);
                    generateReadWritePlot(TYPE[2] + EXT[t], TITLE[2] + " ("
                                    + TIME_DESC[t] + d + ")", timeFrame[t],
                                    false, false);
                    generateReadWritePlot(TYPE[3] + EXT[t], TITLE[3] + " ("
                                    + TIME_DESC[t] + d + ")", timeFrame[t],
                                    true, false);
                    generateConnectionTimePlot(TYPE[4] + EXT[t], TITLE[4]
                                    + " (" + TIME_DESC[t] + d + ")",
                                    timeFrame[t]);
                    generateHitsPlot(TYPE[5] + EXT[t], TITLE[5] + " ("
                                    + TIME_DESC[t] + d + ")", timeFrame[t]);
                    generateCostPlot(TYPE[6] + EXT[t], TITLE[6] + " ("
                                    + TIME_DESC[t] + d + ")", timeFrame[t]);
                }
            } catch (Throwable t) {
                logger.error("error generating billing history plots; quitting ...",
                                t);
                return;
            }

            try {
                logger.debug("sleeping ...");
                Thread.sleep(timeout);
            } catch (InterruptedException ignored) {
            }
        }
    }

    /**
     * Sets the properties for plotting and initializes db access.
     *
     * @throws Throwable
     */
    private void initialize() throws Throwable {
        logger.debug("initializing ...");
        ClassLoader classLoader = Thread.currentThread()
                        .getContextClassLoader();
        initializeProperties(classLoader);
        synchronizeProperties();
        initializeAccess(classLoader);
        TimeFramePlotFactory plotFactory = TimeFramePlotFactory
                        .getInstance(access);
        String impl = properties.getProperty(ITimeFramePlot.FACTORY_TYPE);
        factory = plotFactory.create(impl, properties);
    }

    /**
     * Locates and loads plot properties file.
     *
     * @param classLoader
     * @throws IOException
     */
    private void initializeProperties(ClassLoader classLoader)
                    throws IOException {
        logger.debug("initializeProperties ...");
        String path = null;
        Args args = getArgs();
        if (args != null) {
            path = args.getOpt(PLOT_PROPERTIES_OPT);
        }

        if (path != null && !"".equals(path.trim())) {
            File file = new File(path);
            if (!file.exists()) {
                throw new FileNotFoundException(
                                "Cannot run BillingHistory thread for properties file: "
                                                + file);
            }
            properties.load(new FileInputStream(file));
        } else {
            URL resource = classLoader.getResource(DEFAULT_PROPERTIES);
            if (resource == null) {
                throw new FileNotFoundException(
                                "Cannot run BillingHistory thread for properties resource: "
                                                + resource);
            }
            properties.load(resource.openStream());
        }

        TITLE = new String[7];
        TITLE[0] = properties.getProperty(ITimeFramePlot.TITLE_BYTES_RD);
        TITLE[1] = properties.getProperty(ITimeFramePlot.TITLE_BYTES_WR);
        TITLE[2] = properties.getProperty(ITimeFramePlot.TITLE_TRANSF_RD);
        TITLE[3] = properties.getProperty(ITimeFramePlot.TITLE_TRANSF_WR);
        TITLE[4] = properties.getProperty(ITimeFramePlot.TITLE_CONN_TM);
        TITLE[5] = properties.getProperty(ITimeFramePlot.TITLE_CACHE_HITS);
        TITLE[6] = properties.getProperty(ITimeFramePlot.TITLE_POOL_COST);

        TIME_DESC = new String[4];
        TIME_DESC[0] = properties.getProperty(ITimeFramePlot.DESC_DAILY);
        TIME_DESC[1] = properties.getProperty(ITimeFramePlot.DESC_WEEKLY);
        TIME_DESC[2] = properties.getProperty(ITimeFramePlot.DESC_MONTHLY);
        TIME_DESC[3] = properties.getProperty(ITimeFramePlot.DESC_YEARLY);
    }

    /**
     * Synchronizes args, internal fields and properties
     */
    private void synchronizeProperties() {
        logger.debug("synchronizeProperties ...");
        String plotsTimeout = null;
        Args args = getArgs();
        if (args != null) {
            plotDir = args.getOpt(PLOT_DIR_OPT);
            subDir = args.getOpt(SUB_DIR_OPT);
            imgType = args.getOpt(EXPORT_TYPE_OPT);
            imgExt = args.getOpt(EXPORT_EXT_OPT);
            plotsTimeout = args.getOpt(TIMEOUT_OPT);
        }

        if (plotDir != null) {
            properties.setProperty(ITimeFramePlot.EXPORT_SUBDIR, plotDir);
        } else {
            plotDir = properties.getProperty(ITimeFramePlot.EXPORT_SUBDIR);
        }

        if (plotDir == null) {
            throw new IllegalArgumentException(
                            "Cannot run BillingStatistics thread in interactive mode");
        }

        plotDirF = new File(plotDir);

        if (imgType != null) {
            properties.setProperty(ITimeFramePlot.EXPORT_TYPE, imgType);
        }

        if (imgExt != null) {
            properties.setProperty(ITimeFramePlot.EXPORT_EXTENSION, imgExt);
        } else {
            imgExt = properties.getProperty(ITimeFramePlot.EXPORT_EXTENSION);
        }

        if (plotsTimeout == null) {
            plotsTimeout = properties.getProperty(
                            ITimeFramePlot.THREAD_TIMEOUT_TYPE, DEFAULT_SLEEP);
        }
        timeout = 60000 * Long.parseLong(plotsTimeout);
    }

    /**
     * Initializes database access.
     *
     * @param classLoader
     * @throws BillingInitializationException
     * @throws Throwable
     */
    private void initializeAccess(ClassLoader classLoader)
                    throws NoSuchBeanDefinitionException,
                    BillingInitializationException {
        logger.debug("initializeAccess ...");
        ApplicationContext context = new BillingHistoryApplicationContext();
        if (context != null && context.isSingleton(ACCESS_BEAN)) {
            access = (IBillingInfoAccess) context.getBean(ACCESS_BEAN);
        }
        access.initialize();
        logger.debug("initializeAccess successful");
    }

    /**
     * for daily, weekly, monthly and yearly
     */
    private void generateTimeFrames() {
        Calendar high = TimeFrame.computeHighTimeFromNow(BinType.HOUR);
        timeFrame[0] = getConfiguredTimeFrame(high, BinType.HOUR, Type.DAY);
        high = TimeFrame.computeHighTimeFromNow(BinType.DAY);
        timeFrame[1] = getConfiguredTimeFrame(high, BinType.DAY, Type.WEEK);
        timeFrame[2] = getConfiguredTimeFrame(high, BinType.DAY, Type.MONTH);
        timeFrame[3] = getConfiguredTimeFrame(high, BinType.DAY, Type.YEAR);
    }

    /**
     * Configures the boundaries and interval.
     *
     * @param high
     * @param bin
     * @param type
     * @return timeframe
     */
    private TimeFrame getConfiguredTimeFrame(Calendar high, BinType bin,
                    Type type) {
        TimeFrame timeFrame = new TimeFrame(high.getTimeInMillis());
        timeFrame.setTimebin(bin);
        timeFrame.setTimeframe(type);
        timeFrame.configure();
        return timeFrame;
    }

    /**
     * For bytes and transfer count.
     *
     * @param fileName
     * @param title
     * @param timeFrame
     * @param write
     * @param size
     */
    private void generateReadWritePlot(String fileName, String title,
                    TimeFrame timeFrame, boolean write, boolean size) {
        try {
            ITimeFramePlot plot = factory.createPlot(fileName,
                            new String[] { title });
            logger.debug("generateReadWritePlot created ITimeFramePlot for "
                            + fileName);
            ITimeFrameHistogram[] histogram = createReadWriteHistograms(
                            timeFrame, write, size);
            logger.debug("generateReadWritePlot created histogram set for "
                            + fileName);
            PlotGridPosition pos = new PlotGridPosition(0, 0);
            for (ITimeFrameHistogram h : histogram) {
                plot.addHistogram(pos, h);
            }
            plot.plot();
            logger.debug("generateReadWritePlot completed for " + fileName);
        } catch (Throwable t) {
            logger.error("could not generate " + fileName + " for "
                            + timeFrame.getHigh(), t);
        }
    }

    /**
     * For dCache connection durations.
     *
     * @param fileName
     * @param title
     * @param timeFrame
     */
    private void generateConnectionTimePlot(String fileName, String title,
                    TimeFrame timeFrame) {
        try {
            ITimeFramePlot plot = factory.createPlot(fileName,
                            new String[] { title });
            ITimeFrameHistogram[] histogram = factory
                            .createDcConnectTimeHistograms(timeFrame);
            PlotGridPosition pos = new PlotGridPosition(0, 0);
            for (ITimeFrameHistogram h : histogram) {
                plot.addHistogram(pos, h);
            }
            plot.plot();
            logger.debug("generateConnectionTimePlot completed for " + fileName);
        } catch (Throwable t) {
            logger.error("could not generate " + fileName + " for "
                            + timeFrame.getHigh(), t);
        }
    }

    /**
     * Cache hit counts.
     *
     * @param fileName
     * @param title
     * @param timeFrame
     */
    private void generateHitsPlot(String fileName, String title,
                    TimeFrame timeFrame) {
        try {
            ITimeFramePlot plot = factory.createPlot(fileName,
                            new String[] { title });
            ITimeFrameHistogram[] histogram = factory
                            .createHitHistograms(timeFrame);
            PlotGridPosition pos = new PlotGridPosition(0, 0);
            for (ITimeFrameHistogram h : histogram) {
                plot.addHistogram(pos, h);
            }
            plot.plot();
            logger.debug("generateHitsPlot completed for " + fileName);
        } catch (Throwable t) {
            logger.error("could not generate " + fileName + " for "
                            + timeFrame.getHigh(), t);
        }
    }

    /**
     * Pool cost.
     *
     * @param fileName
     * @param title
     * @param timeFrame
     */
    private void generateCostPlot(String fileName, String title,
                    TimeFrame timeFrame) {
        try {
            ITimeFramePlot plot = factory.createPlot(fileName,
                            new String[] { title });
            ITimeFrameHistogram h = factory.createCostHistogram(timeFrame);
            PlotGridPosition pos = new PlotGridPosition(0, 0);
            plot.addHistogram(pos, h);
            plot.plot();
            logger.debug("generateCostPlot completed for " + fileName);
        } catch (Throwable t) {
            logger.error("could not generate " + fileName + " for "
                            + timeFrame.getHigh(), t);
        }
    }

    /**
     * Auxiliary for histograms differentiating reads and writes.
     *
     * @param timeFrame
     * @param write
     * @param size
     * @return pair of histograms (dCache, HSM)
     * @throws Throwable
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

    /**
     * @return the running
     */
    public synchronized boolean isRunning() {
        return running;
    }

    /**
     * @param running
     *            the running to set
     */
    public synchronized void setRunning(boolean running) {
        this.running = running;
    }

    public void close()
    {
        access.close();
    }

    /**
     * @return the args
     */
    public Args getArgs() {
        return args;
    }
}
