/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.services.billing.plots;

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

import org.dcache.services.billing.cells.BillingDatabase;
import org.dcache.services.billing.db.IBillingInfoAccess;
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

/**
 * The daemon which handles periodic refreshing of the billing plots from the
 * database. (This code is refactored from the previous version
 * {@link BillingDatabase}, which now represents the object which solely handles
 * the cell messages and which starts and stops the optional threads.
 *
 * @author arossi
 */
public class BillingInfoHistogramGenerator extends Thread {

    private static final Logger logger
        = LoggerFactory.getLogger(BillingInfoHistogramGenerator.class);

    /*
     * for histogram plotting
     */
    private static final List<String> TYPE = ImmutableList.of("bytes_rd",
                    "bytes_wr", "transfers_rd", "transfers_wr", "time", "hits");
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
        TimeFrame[] timeFrame = new TimeFrame[4];
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
        TimeFrame timeFrame = new TimeFrame(high.getTimeInMillis());
        timeFrame.setTimebin(bin);
        timeFrame.setTimeframe(type);
        timeFrame.configure();
        return timeFrame;
    }

    private static String getTypeName(int type, int tFrame) {
        return TYPE.get(type) + EXT.get(tFrame);
    }

    /*
     * injected
     */
    private IBillingInfoAccess access;
    private String propertiesPath;
    private String plotsDir;
    private String imgType;

    /*
     * state
     */
    private ITimeFrameHistogramFactory factory;
    private List<String> timeDescription;
    private Long timeout;
    private boolean running;

    /**
     * Sleeps for the given timeout, then regenerates the plots.
     */
    @Override
    public void run() {
        try {
            logger.debug("initializing plotting ...");
            Properties properties = new Properties();
            ClassLoader classLoader
                = Thread.currentThread().getContextClassLoader();
            initializeProperties(classLoader, properties);
            synchronizePlotProperties(properties);
            TimeFramePlotFactory plotFactory
                = TimeFramePlotFactory.getInstance(access);
            String impl = properties.getProperty(ITimeFramePlot.FACTORY_TYPE);
            factory = plotFactory.create(impl, properties);
        } catch (Exception t) {
            logger.error("initialization failed; thread cannot run", t);
            return;
        }

        logger.debug("starting run ...");
        setRunning(true);

        boolean[] isWrite = new boolean[] { false, true, false, true };
        boolean[] isSize = new boolean[] { true, true, false, false };

        while (isRunning()) {
            try {
                logger.debug("generating time frames ...");
                TimeFrame[] timeFrames = generateTimeFrames();

                logger.debug("generating plots ...");
                for (int tFrame = 0; tFrame < timeFrames.length; tFrame++) {
                    Date low = timeFrames[tFrame].getLow();
                    int type = 0;
                    for (; type < 4; type++) {
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
                }
            } catch (Exception t) {
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
        }
    }

    /*
     * Auxiliary for histograms differentiating reads and writes.
     */
    private ITimeFrameHistogram[] createReadWriteHistograms(
                    TimeFrame timeFrame, boolean write, boolean size)
                    throws Exception {
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
            ITimeFramePlot plot = factory.createPlot(fileName,
                            new String[] { title });
            ITimeFrameHistogram[] histogram
                = factory.createDcConnectTimeHistograms(timeFrame);
            PlotGridPosition pos = new PlotGridPosition(0, 0);
            for (final ITimeFrameHistogram h : histogram) {
                plot.addHistogram(pos, h);
            }
            plot.plot();
            logger.debug("generateConnectionTimePlot completed for " + fileName);
        } catch (Exception t) {
            logger.error("could not generate " + fileName + " for "
                            + timeFrame.getHigh(), t);
        }
    }

    private void generateHitsPlot(String fileName, String title,
                    TimeFrame timeFrame) {
        try {
            ITimeFramePlot plot = factory.createPlot(fileName,
                            new String[] { title });
            ITimeFrameHistogram[] histogram
                = factory.createHitHistograms(timeFrame);
            PlotGridPosition pos = new PlotGridPosition(0, 0);
            for (final ITimeFrameHistogram h : histogram) {
                plot.addHistogram(pos, h);
            }
            plot.plot();
            logger.debug("generateHitsPlot completed for " + fileName);
        } catch (Exception t) {
            logger.error("could not generate " + fileName + " for "
                            + timeFrame.getHigh(), t);
        }
    }

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
        } catch (Exception t) {
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
            File file = new File(propertiesPath);
            if (!file.exists()) {
                throw new FileNotFoundException(
                                "Cannot run plotting thread for properties file: "
                                                + file);
            }
            InputStream stream = new FileInputStream(file);
            try {
                properties.load(stream);
            } finally {
                stream.close();
            }
        } else {
            URL resource = classLoader.getResource(DEFAULT_PROPERTIES);
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
            final String value = properties.getProperty(
                            ITimeFramePlot.THREAD_TIMEOUT_TYPE,
                            DEFAULT_PLOT_REFRESH_IN_MINUTES);
            timeout = TimeUnit.MINUTES.toMillis(Long.parseLong(value));
        }
    }
}
