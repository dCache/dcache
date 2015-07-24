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
package org.dcache.webadmin.controller.impl;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.wicket.util.lang.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.ServiceUnavailableException;
import diskCacheV111.util.TimeoutCacheException;

import dmg.cells.nucleus.NoRouteToCellException;
import org.dcache.cells.CellStub;
import org.dcache.services.billing.histograms.ITimeFrameHistogramFactory;
import org.dcache.services.billing.histograms.ITimeFrameHistogramFactory.Style;
import org.dcache.services.billing.histograms.TimeFrame;
import org.dcache.services.billing.histograms.TimeFrame.BinType;
import org.dcache.services.billing.histograms.TimeFrame.Type;
import org.dcache.services.billing.histograms.config.HistogramWrapper;
import org.dcache.services.billing.histograms.data.ITimeFrameHistogramDataService;
import org.dcache.services.billing.histograms.data.TimeFrameHistogramData;
import org.dcache.services.billing.histograms.data.TimeFrameHistogramDataProxy;
import org.dcache.services.billing.plots.util.ITimeFramePlot;
import org.dcache.services.billing.plots.util.ITimeFramePlotGenerator;
import org.dcache.services.billing.plots.util.PlotGridPosition;
import org.dcache.services.billing.plots.util.PlotProperties;
import org.dcache.services.billing.plots.util.TimeFramePlotProperties;
import org.dcache.services.billing.plots.util.TimeFramePlotProperties.PlotType;
import org.dcache.webadmin.controller.IBillingService;

/**
 * Uses {@link TimeFrameHistogramDataClient} to retrieve the billing plot data
 * when it is stale (file timestamp is older than the specified interval from
 * present). Uses {@link ITimeFrameHistogramFactory} to convert the histogram
 * data into implementation-specific histograms, and
 * {@link ITimeFramePlotGenerator} to write the plots to the specified location.
 *
 * @author arossi
 */
public final class StandardBillingService implements IBillingService, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(StandardBillingService.class);
    private static final double ERRORS_PER_SECOND = 1.0 / 120.0;

    /**
     * injected
     */
    private CellStub cell;
    private PlotProperties properties;
    private String plotsDir;

    private String imgType;
    private ITimeFrameHistogramDataService client;

    /**
     * in default setup, these are implemented by same class
     */
    private ITimeFrameHistogramFactory factory;
    private ITimeFramePlotGenerator generator;

    /**
     * populated during initialization
     */
    private final List<String> plotFilePrefix = new ArrayList<>();
    private final List<String> plotTitles = new ArrayList<>();
    private final List<String> extTypes = new ArrayList<>();
    private final List<String> timeDescription = new ArrayList<>();

    /**
     * refreshing can be done periodically by the daemon, or forced
     * through the web interface directly
     */
    private final RateLimiter rate = RateLimiter.create(ERRORS_PER_SECOND);

    private long timeout;
    private int popupWidth;
    private int popupHeight;
    private long lastUpdate = System.currentTimeMillis();
    private Thread refresher;

    public List<TimeFrameHistogramData> load(PlotType plotType,
                    TimeFrame timeFrame) throws ServiceUnavailableException,
                                                NoRouteToCellException,
                                                TimeoutCacheException {
        logger.debug("remote fetch of {} {}", plotType, timeFrame);
        List<TimeFrameHistogramData> histograms = new ArrayList<>();
        try {
            switch (plotType) {
                case BYTES_READ:
                    add(client.getDcBytesHistogram(timeFrame, false),
                                    histograms);
                    add(client.getHsmBytesHistogram(timeFrame, false),
                                    histograms);
                    break;
                case BYTES_WRITTEN:
                    add(client.getDcBytesHistogram(timeFrame, true),
                                    histograms);
                    add(client.getHsmBytesHistogram(timeFrame, true),
                                    histograms);
                    break;
                case BYTES_P2P:
                    add(client.getP2pBytesHistogram(timeFrame),
                                    histograms);
                    break;
                case TRANSFERS_READ:
                    add(client.getDcTransfersHistogram(timeFrame, false),
                                    histograms);
                    add(client.getHsmTransfersHistogram(timeFrame, false),
                                    histograms);
                    break;
                case TRANSFERS_WRITTEN:
                    add(client.getDcTransfersHistogram(timeFrame, true),
                                    histograms);
                    add(client.getHsmTransfersHistogram(timeFrame, true),
                                    histograms);
                    break;
                case TRANSFERS_P2P:
                    add(client.getP2pTransfersHistogram(timeFrame),
                                    histograms);
                    break;
                case CONNECTION_TIME:
                    add(client.getDcConnectTimeHistograms(timeFrame),
                                    histograms);
                    break;
                case CACHE_HITS:
                    add(client.getHitHistograms(timeFrame),
                                    histograms);
                    break;
            }
        } catch (UndeclaredThrowableException ute) {
            Throwable cause
                = Exceptions.findCause(ute, ServiceUnavailableException.class);
            if (cause != null) {
                throw (ServiceUnavailableException)cause;
            }
            cause = Exceptions.findCause(ute, NoRouteToCellException.class);
            if (cause != null) {
                throw (NoRouteToCellException)cause;
            }
            cause = Exceptions.findCause(ute, TimeoutCacheException.class);
            if (cause != null) {
                throw (TimeoutCacheException)cause;
            }
            cause = ute.getCause();
            Throwables.propagateIfPossible(cause);
            throw new RuntimeException("Unexpected error: "
                                        + "this is probably a bug. Please report "
                                        + "to the dCache team.",
                                        cause);
        }
        return histograms;
    }

    private static void add(TimeFrameHistogramData[] returned,
                    List<TimeFrameHistogramData> histograms) {
        if (returned != null) {
            for (TimeFrameHistogramData d : returned) {
                histograms.add(d);
            }
        }
    }

    /**
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

    /**
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

    private final Style style;
    private final String scale;

    public StandardBillingService(String style, String scale) {
        this.style = Style.valueOf(style);
        this.scale = scale;
    }

    @Override
    public File[] getImageFiles() {
        List<File> files = new ArrayList<>();
        for (int i = 0; i < plotFilePrefix.size(); i++) {
            for (int j = 0; j < extTypes.size(); j++) {
                File img = new File(plotsDir, getFileName(i, j) + "." + imgType);
                files.add(img);
            }
        }
        return files.toArray(new File[files.size()]);
    }

    @Override
    public long getLastUpdate() {
        return lastUpdate;
    }

    @Override
    public int getPopupHeight() {
        return popupHeight;
    }

    @Override
    public int getPopupWidth() {
        return popupWidth;
    }

    @Override
    public long getRefreshIntervalInMillis() {
        return timeout;
    }

    @Override
    public void initialize() {
        synchronizeTimeFramePlotProperties();

        for (TimeFramePlotProperties.PlotType pt : TimeFramePlotProperties.PlotType.values()) {
            plotFilePrefix.add(properties.getProperty(TimeFramePlotProperties.PLOT_TYPE
                            + pt));
            plotTitles.add(properties.getProperty(TimeFramePlotProperties.PLOT_TITLE
                            + pt));
        }

        for (TimeFramePlotProperties.TimeFrame tf : TimeFramePlotProperties.TimeFrame.values()) {
            extTypes.add(properties.getProperty(TimeFramePlotProperties.TIME_EXT
                            + tf));
            timeDescription.add(properties.getProperty(TimeFramePlotProperties.TIME_DESC
                            + tf));
        }

        Properties jProperties = properties.toJavaProperties();
        factory.setProperties(jProperties);
        generator.initialize(jProperties);

        client = (ITimeFrameHistogramDataService) Proxy.newProxyInstance(
                        Thread.currentThread().getContextClassLoader(),
                        new Class[] { ITimeFrameHistogramDataService.class },
                        new TimeFrameHistogramDataProxy(cell));

        refresher = new Thread(this, "StandardBillingServiceRefresher");
        refresher.start();
    }

    @Override
    public void refresh() throws NoRouteToCellException,
                                 TimeoutCacheException,
                                 ServiceUnavailableException{
        TimeFrame[] timeFrames = generateTimeFrames();
        for (int tFrame = 0; tFrame < timeFrames.length; tFrame++) {
            Date low = timeFrames[tFrame].getLow();
            for (PlotType type : PlotType.values()) {
                String fileName = getFileName(type.ordinal(), tFrame);
                generatePlot(type, timeFrames[tFrame], fileName,
                                getTitle(type.ordinal(), tFrame, low));
            }
        }
        lastUpdate = System.currentTimeMillis();
    }

    @Override
    public void run() {
        try {
            while (true) {
                try {
                    refresh();
                    Thread.sleep(timeout);
                } catch (ServiceUnavailableException e) {
                    logger.error("The billing database has been disabled."
                                    + "  To generate plots, please restart the service when"
                                    + " the billing database is once again available");
                    break;
                } catch (NoRouteToCellException e) {
                    if (rate.tryAcquire()) {
                        logger.warn("No route to the billing service yet; "
                                        + "retrying every 10 seconds");
                    }
                    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                } catch (TimeoutCacheException e) {
                    if (rate.tryAcquire()) {
                        logger.warn("Billing service currently unreachable; "
                                        + "retrying after timeout ...");
                    }
                    Thread.sleep(timeout);
                }
            }
        } catch (InterruptedException interrupted) {
            logger.trace("{} interrupted; exiting ...", refresher);
        }
    }

    public void setCell(CellStub cell) {
        this.cell = cell;
    }

    public void setFactory(ITimeFrameHistogramFactory factory) {
        this.factory = factory;
    }

    public void setGenerator(ITimeFramePlotGenerator generator) {
        this.generator = generator;
    }

    public void setImgType(String imgType) {
        this.imgType = imgType;
    }

    public void setPlotProperties(PlotProperties properties) {
        this.properties = properties;
    }

    public void setPlotsDir(String plotsDir) {
        this.plotsDir = plotsDir;
    }

    public void shutDown() {
        if (refresher != null) {
            refresher.interrupt();
        }
    }

    private void generatePlot(PlotType type, TimeFrame timeFrame,
                    String fileName, String title) throws ServiceUnavailableException,
                                                          TimeoutCacheException,
                                                          NoRouteToCellException {
        List<TimeFrameHistogramData> data = load(type, timeFrame);
        List<HistogramWrapper<?>> config = new ArrayList<>();
        int i = 0;
        for (TimeFrameHistogramData d : data) {
            if (i == 0 && style == Style.OUTLINE) {
                config.add(factory.getConfigurationFor(timeFrame, d,
                                Style.FILLED, scale));
            } else {
                config.add(factory.getConfigurationFor(timeFrame, d, style,
                                scale));
            }
            ++i;
        }

        PlotGridPosition pos = new PlotGridPosition(0, 0);
        ITimeFramePlot plot = generator.createPlot(fileName,
                        new String[] { title }, pos, config);

        plot.plot();
    }

    private String getFileName(int type, int tFrame) {
        return plotFilePrefix.get(type) + extTypes.get(tFrame) + "-" + style
                        + "-" + scale;
    }

    private String getTitle(int type, int tFrame, Date low) {
        return plotTitles.get(type) + " (" + timeDescription.get(tFrame) + " "
                        + low + ")";
    }

    /**
     * makes sure overwritten properties (if any) are correctly propagated and
     * that internal properties correspond to those in the definition file if
     * the former are undefined.
     */
    private void synchronizeTimeFramePlotProperties() {
        if (plotsDir != null) {
            properties.setProperty(TimeFramePlotProperties.EXPORT_SUBDIR,
                            plotsDir);
        } else {
            plotsDir = properties.getProperty(TimeFramePlotProperties.EXPORT_SUBDIR);
        }

        if (plotsDir == null) {
            throw new IllegalArgumentException("plots directory is undefined");
        }

        if (imgType != null) {
            properties.setProperty(TimeFramePlotProperties.EXPORT_TYPE, imgType);
        } else {
            imgType = properties.getProperty(TimeFramePlotProperties.EXPORT_TYPE);
        }

        if (imgType == null) {
            throw new IllegalArgumentException("image type is undefined");
        }

        properties.setProperty(TimeFramePlotProperties.EXPORT_EXTENSION, "."
                        + imgType);

        String value = properties.getProperty(TimeFramePlotProperties.REFRESH_THRESHOLD);
        TimeUnit unit = TimeUnit.valueOf(properties.getProperty
                        (TimeFramePlotProperties.REFRESH_THRESHOLD_UNIT));
        timeout = unit.toMillis(Long.parseLong(value));

        value = properties.getProperty(TimeFramePlotProperties.POPUP_WIDTH);
        popupWidth = Integer.parseInt(value);

        value = properties.getProperty(TimeFramePlotProperties.POPUP_HEIGHT);
        popupHeight = Integer.parseInt(value);

        String resource = generator.getPropertiesResource();
        if (resource != null) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            URL url = classLoader.getResource(resource);
            if (url != null) {
                Properties p = new Properties();
                try (InputStream is = url.openStream()) {
                    p.load(is);
                    properties.override(p);
                } catch (IOException t) {
                    throw new RuntimeException("problem loading properties: "
                                    + t.getMessage(), t);
                }
            } else {
                throw new RuntimeException("resource " + resource
                                + " could not be located");
            }
        }

        logger.debug("plot properties are {}", properties.toJavaProperties());
    }
}
