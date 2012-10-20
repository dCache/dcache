package org.dcache.services.billing.plots.jaida;

import hep.aida.IAnalysisFactory;
import hep.aida.IHistogramFactory;
import hep.aida.ITree;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.BaseDaily;
import org.dcache.services.billing.db.data.DcacheReadsDaily;
import org.dcache.services.billing.db.data.DcacheTimeDaily;
import org.dcache.services.billing.db.data.DcacheWritesDaily;
import org.dcache.services.billing.db.data.HSMReadsDaily;
import org.dcache.services.billing.db.data.HSMWritesDaily;
import org.dcache.services.billing.db.data.HitsDaily;
import org.dcache.services.billing.db.data.IPlotData;
import org.dcache.services.billing.db.data.MoverData;
import org.dcache.services.billing.db.data.PnfsConnectInfo;
import org.dcache.services.billing.db.data.PnfsStorageInfo;
import org.dcache.services.billing.db.data.PoolHitData;
import org.dcache.services.billing.db.data.SizeDaily;
import org.dcache.services.billing.db.data.StorageData;
import org.dcache.services.billing.plots.exceptions.TimeFrameFactoryInitializationException;
import org.dcache.services.billing.plots.exceptions.TimeFramePlotException;
import org.dcache.services.billing.plots.util.AbstractTimeFrameHistogramFactory;
import org.dcache.services.billing.plots.util.ITimeFrameHistogram;
import org.dcache.services.billing.plots.util.ITimeFramePlot;
import org.dcache.services.billing.plots.util.TimeFrame;
import org.dcache.services.billing.plots.util.TimeFrame.BinType;

/**
 * Wraps IHistogramFactory.
 *
 * @see IHistogramFactory
 * @author arossi
 */
public final class JaidaTimeFrameHistogramFactory extends
                AbstractTimeFrameHistogramFactory {

    /**
     * Stand-in aggregate object for hits data.
     */
    private static class HourlyHitData extends BaseDaily {
        private Long cached = 0L;
        private Long notcached = 0L;

        @Override
        public Map<String, Double> data() {
            final Map<String, Double> dataMap = super.data();
            dataMap.put(HitsDaily.CACHED, cached.doubleValue());
            dataMap.put(HitsDaily.NOT_CACHED, notcached.doubleValue());
            return dataMap;
        }

        @Override
        public String toString() {
            return "(" + dateString() + "," + cached + "," + notcached + ")";
        }
    }

    private IAnalysisFactory af;
    private IHistogramFactory factory;
    private ITree tree;
    private Properties properties;

    public JaidaTimeFrameHistogramFactory() {
    }

    @Override
    public ITimeFrameHistogram createDcBytesHistogram(TimeFrame timeFrame,
                    boolean write) throws TimeFramePlotException {
        final String title = write ? getProperty(ITimeFramePlot.LABEL_DC_WR)
                        : getProperty(ITimeFramePlot.LABEL_DC_RD);
        final ITimeFrameHistogram histogram
            = new JaidaTimeFrameHistogram(factory, timeFrame, title);
        histogram.setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
        histogram.setScaling(getProperty(ITimeFramePlot.SCALE_BYTES));
        histogram.setColor(getProperty(ITimeFramePlot.COLOR_DC));
        Collection<IPlotData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_GBYTES_HR));
            plotData = getFineGrainedPlotData(MoverData.class, timeFrame,
                            "isNew", "java.lang.Boolean", write);
            histogram.setData(plotData, MoverData.TRANSFER_SIZE, GB);
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_GBYTES_DY));
            if (write) {
                plotData = getCoarseGrainedPlotData(DcacheWritesDaily.class,
                                timeFrame);
                histogram.setData(plotData, SizeDaily.SIZE, GB);
            } else {
                plotData = getCoarseGrainedPlotData(DcacheReadsDaily.class,
                                timeFrame);
                histogram.setData(plotData, SizeDaily.SIZE, GB);
            }
        }
        return histogram;
    }

    @Override
    public ITimeFrameHistogram[] createDcConnectTimeHistograms(
                    TimeFrame timeFrame) throws TimeFramePlotException {
        Collection<IPlotData> plotData;

        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = getHourlyAggregateForTime(timeFrame);
        } else {
            plotData = getCoarseGrainedPlotData(DcacheTimeDaily.class, timeFrame);
        }

        final String[] title = new String[] {
                        getProperty(ITimeFramePlot.LABEL_MIN),
                        getProperty(ITimeFramePlot.LABEL_MAX),
                        getProperty(ITimeFramePlot.LABEL_AVG) };
        final String[] field = new String[] { DcacheTimeDaily.MIN_TIME,
                        DcacheTimeDaily.MAX_TIME, DcacheTimeDaily.AVG_TIME };
        final String[] color = new String[] {
                        getProperty(ITimeFramePlot.COLOR_MIN),
                        getProperty(ITimeFramePlot.COLOR_MAX),
                        getProperty(ITimeFramePlot.COLOR_AVG) };

        final ITimeFrameHistogram[] histogram = new ITimeFrameHistogram[3];

        for (int h = 0; h < histogram.length; h++) {
            histogram[h] = new JaidaTimeFrameHistogram(factory,
                                                       timeFrame,
                                                       title[h]);
            histogram[h].setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
            histogram[h].setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_CONNECT));
            histogram[h].setColor(color[h]);
            histogram[h].setScaling(getProperty(ITimeFramePlot.SCALE_TIME));
            histogram[h].setData(plotData, field[h],
                                    1.0 * TimeUnit.MINUTES.toMillis(1));
        }
        return histogram;
    }

    @Override
    public ITimeFrameHistogram createDcTransfersHistogram(TimeFrame timeFrame,
                    boolean write) throws TimeFramePlotException {
        final String title = write ? getProperty(ITimeFramePlot.LABEL_DC_WR)
                        : getProperty(ITimeFramePlot.LABEL_DC_RD);
        final ITimeFrameHistogram histogram = new JaidaTimeFrameHistogram(
                        factory, timeFrame, title);
        histogram.setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
        histogram.setScaling(getProperty(ITimeFramePlot.SCALE_COUNT));
        histogram.setColor(getProperty(ITimeFramePlot.COLOR_DC));
        Collection<IPlotData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_TRANSF_HR));

            plotData = getFineGrainedPlotData(MoverData.class, timeFrame,
                                              "isNew", "java.lang.Boolean",
                                              write);
            histogram.setData(plotData, null, null);
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_TRANSF_DY));
            if (write) {
                plotData = getCoarseGrainedPlotData(DcacheWritesDaily.class,
                                timeFrame);
                histogram.setData(plotData, BaseDaily.COUNT, null);
            } else {
                plotData = getCoarseGrainedPlotData(DcacheReadsDaily.class,
                                timeFrame);
                histogram.setData(plotData, BaseDaily.COUNT, null);
            }
        }
        return histogram;
    }

    @Override
    public ITimeFrameHistogram[] createHitHistograms(TimeFrame timeFrame)
                    throws TimeFramePlotException {
        Collection<IPlotData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = getHourlyAggregateForHits(timeFrame);
        } else {
            plotData = getCoarseGrainedPlotData(HitsDaily.class, timeFrame);
        }
        final ITimeFrameHistogram[] histogram = new ITimeFrameHistogram[2];
        histogram[0] = new JaidaTimeFrameHistogram(factory, timeFrame,
                        getProperty(ITimeFramePlot.LABEL_CACHED));
        histogram[0].setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
        histogram[0].setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_HITS_HR));
        histogram[0].setScaling(getProperty(ITimeFramePlot.SCALE_COUNT));
        histogram[0].setColor(getProperty(ITimeFramePlot.COLOR_CACHED));
        histogram[0].setData(plotData, HitsDaily.CACHED, null);
        histogram[1] = new JaidaTimeFrameHistogram(factory, timeFrame,
                        getProperty(ITimeFramePlot.LABEL_NCACHED));
        histogram[1].setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
        histogram[1].setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_HITS_DY));
        histogram[1].setScaling(getProperty(ITimeFramePlot.SCALE_COUNT));
        histogram[1].setColor(getProperty(ITimeFramePlot.COLOR_NCACHED));
        histogram[1].setData(plotData, HitsDaily.NOT_CACHED, null);
        return histogram;
    }

    @Override
    public ITimeFrameHistogram createHsmBytesHistogram(TimeFrame timeFrame,
                    boolean write) throws TimeFramePlotException {
        final String title = write ? getProperty(ITimeFramePlot.LABEL_HSM_WR)
                        : getProperty(ITimeFramePlot.LABEL_HSM_RD);
        final ITimeFrameHistogram histogram
            = new JaidaTimeFrameHistogram(factory, timeFrame, title);
        histogram.setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
        histogram.setScaling(getProperty(ITimeFramePlot.SCALE_BYTES));
        histogram.setColor(getProperty(ITimeFramePlot.COLOR_HSM));
        Collection<IPlotData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_GBYTES_HR));
            plotData = getFineGrainedPlotData(StorageData.class, timeFrame,
                                              "action", "java.lang.String",
                                              write ? "store" : "restore");
            histogram.setData(plotData, PnfsStorageInfo.FULL_SIZE, GB);
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_GBYTES_DY));
            if (write) {
                plotData = getCoarseGrainedPlotData(HSMWritesDaily.class, timeFrame);
                histogram.setData(plotData, SizeDaily.SIZE, GB);
            } else {
                plotData = getCoarseGrainedPlotData(HSMReadsDaily.class, timeFrame);
                histogram.setData(plotData, SizeDaily.SIZE, GB);
            }
        }
        return histogram;
    }

    @Override
    public ITimeFrameHistogram createHsmTransfersHistogram(TimeFrame timeFrame,
                    boolean write) throws TimeFramePlotException {
        final String title = write ? getProperty(ITimeFramePlot.LABEL_HSM_WR)
                        : getProperty(ITimeFramePlot.LABEL_HSM_RD);
        final ITimeFrameHistogram histogram
            = new JaidaTimeFrameHistogram(factory, timeFrame, title);
        histogram.setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
        histogram.setScaling(getProperty(ITimeFramePlot.SCALE_COUNT));
        histogram.setColor(getProperty(ITimeFramePlot.COLOR_HSM));
        Collection<IPlotData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_TRANSF_HR));
            plotData = getFineGrainedPlotData(StorageData.class, timeFrame,
                                              "action", "java.lang.String",
                                              write ? "store" : "restore");
            histogram.setData(plotData, null, null);

        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_TRANSF_DY));
            if (write) {
                plotData = getCoarseGrainedPlotData(HSMWritesDaily.class, timeFrame);
                histogram.setData(plotData, BaseDaily.COUNT, null);
            } else {
                plotData = getCoarseGrainedPlotData(HSMReadsDaily.class, timeFrame);
                histogram.setData(plotData, BaseDaily.COUNT, null);
            }
        }
        return histogram;
    }

    @Override
    public ITimeFramePlot createPlot(String plotName, String[] subtitles) {
        return new JaidaTimeFramePlot(af, tree, plotName, subtitles, properties);
    }

    @Override
    public void initialize(IBillingInfoAccess access)
                    throws TimeFrameFactoryInitializationException {
        initialize(access, (String) null);
    }

    @Override
    public void initialize(IBillingInfoAccess access, Properties properties)
                    throws TimeFrameFactoryInitializationException {
        super.initialize(access);
        af = IAnalysisFactory.create();
        tree = af.createTreeFactory().create();
        factory = af.createHistogramFactory(tree);
        this.properties = properties;
    }

    @Override
    public void initialize(IBillingInfoAccess access, String propertiesPath)
                    throws TimeFrameFactoryInitializationException {
        super.initialize(access);
        af = IAnalysisFactory.create();
        tree = af.createTreeFactory().create();
        factory = af.createHistogramFactory(tree);
        properties = new Properties();

        try {
            setDefaults();
            if (propertiesPath != null) {
                try (InputStream stream = new FileInputStream(new File(propertiesPath))) {
                    properties.load(stream);
                }

            }
        } catch (final Throwable t) {
            throw new TimeFrameFactoryInitializationException(t);
        }
    }

    private Collection<IPlotData> getHourlyAggregateForHits(TimeFrame timeFrame)
                    throws TimeFramePlotException {
        Collection<IPlotData> plotData = getFineGrainedPlotData(
                        PoolHitData.class, timeFrame);
        final Map<String, HourlyHitData> hourlyAggregate
            = new TreeMap<String, HourlyHitData>();
        for (final IPlotData d : plotData) {
            final boolean cached = ((PoolHitData) d).getFileCached();
            final Date date = normalizeForHour(d.timestamp());
            final String key = date.toString();
            HourlyHitData hourlyData = hourlyAggregate.get(key);
            if (hourlyData == null) {
                hourlyData = new HourlyHitData();
                hourlyData.setDate(date);
                hourlyAggregate.put(key, hourlyData);
            }
            if (cached) {
                hourlyData.cached++;
            } else {
                hourlyData.notcached++;
            }
        }
        plotData = new ArrayList<IPlotData>();
        plotData.addAll(hourlyAggregate.values());
        return plotData;
    }

    /**
     * Does hourly aggregation of a bounded set of billing info data points in
     * memory.
     */
    private Collection<IPlotData> getHourlyAggregateForTime(TimeFrame timeFrame)
                    throws TimeFramePlotException {
        Collection<IPlotData> plotData = getFineGrainedPlotData(
                        MoverData.class, timeFrame);
        final Map<String, DcacheTimeDaily> hourlyAggregate
            = new TreeMap<String, DcacheTimeDaily>();

        for (final IPlotData d : plotData) {
            final Date date = normalizeForHour(d.timestamp());
            final String key = date.toString();
            DcacheTimeDaily hourlyData = hourlyAggregate.get(key);
            if (hourlyData == null) {
                hourlyData = new DcacheTimeDaily();
                hourlyData.setDate(date);
                hourlyAggregate.put(key, hourlyData);
            }
            final long time
                = d.data().get(PnfsConnectInfo.CONNECTION_TIME).longValue();
            hourlyData.setCount(hourlyData.getCount() + 1);
            hourlyData.setTotalTime(hourlyData.getTotalTime() + time);
            final long min = hourlyData.getMinimum();
            if (0 != min) {
                hourlyData.setMinimum(Math.min(hourlyData.getMinimum(), time));
            }
            hourlyData.setMaximum(Math.max(hourlyData.getMaximum(), time));
        }

        plotData = new ArrayList<IPlotData>();
        plotData.addAll(hourlyAggregate.values());
        return plotData;
    }

    /**
     * @return property value, or <code>null</code> if undefined.
     */
    private String getProperty(String name) {
        return properties.getProperty(name, null);
    }

    /**
     * Rounds down to beginning of the hour in which the timestamp is bounded.
     *
     * @return normalized date (hh:00:00.000)
     */
    private Date normalizeForHour(Date date) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    /**
     * Standard settings for plot look and feel
     */
    private void setDefaults() {
        properties.setProperty(ITimeFramePlot.PLOT_TITLE_COLOR, "black");
        properties.setProperty(ITimeFramePlot.PLOT_TITLE_SIZE, "16");
        properties.setProperty(ITimeFramePlot.PLOT_GRID_ROWS, "1");
        properties.setProperty(ITimeFramePlot.PLOT_GRID_COLS, "1");
        properties.setProperty(ITimeFramePlot.PLOT_WIDTH, "1000");
        properties.setProperty(ITimeFramePlot.PLOT_HEIGHT, "500");
        properties.setProperty(ITimeFramePlot.CURVE_THICKNESS, "3");
        properties.setProperty(ITimeFramePlot.MARKER_SHAPE, "box");
        properties.setProperty(ITimeFramePlot.MARKER_SHAPE, "9");
        properties.setProperty(ITimeFramePlot.X_AXIS_TYPE, "date");
        properties.setProperty(ITimeFramePlot.X_AXIS_SIZE, "14");
        properties.setProperty(ITimeFramePlot.X_AXIS_LABEL_COLOR, "black");
        properties.setProperty(ITimeFramePlot.X_AXIS_TICK_SIZE, "10");
        properties.setProperty(ITimeFramePlot.X_AXIS_TICK_LABEL_COLOR, "black");
        properties.setProperty(ITimeFramePlot.Y_AXIS_SIZE, "14");
        properties.setProperty(ITimeFramePlot.Y_AXIS_LABEL_COLOR, "black");
        properties.setProperty(ITimeFramePlot.Y_AXIS_TICK_SIZE, "10");
        properties.setProperty(ITimeFramePlot.Y_AXIS_TICK_LABEL_COLOR, "black");
        properties.setProperty(ITimeFramePlot.Y_AXIS_ALLOW_ZERO_SUPPRESSION,
                        "false");
    }
}
