package org.dcache.services.billing.plots.jaida;

import hep.aida.IAnalysisFactory;
import hep.aida.IHistogramFactory;
import hep.aida.ITree;

import java.io.File;
import java.io.FileInputStream;
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
import org.dcache.services.billing.db.data.CostDaily;
import org.dcache.services.billing.db.data.DcacheReadsDaily;
import org.dcache.services.billing.db.data.DcacheReadsHourly;
import org.dcache.services.billing.db.data.DcacheTimeDaily;
import org.dcache.services.billing.db.data.DcacheTimeHourly;
import org.dcache.services.billing.db.data.DcacheWritesDaily;
import org.dcache.services.billing.db.data.DcacheWritesHourly;
import org.dcache.services.billing.db.data.HSMReadsDaily;
import org.dcache.services.billing.db.data.HSMReadsHourly;
import org.dcache.services.billing.db.data.HSMWritesDaily;
import org.dcache.services.billing.db.data.HSMWritesHourly;
import org.dcache.services.billing.db.data.HitsDaily;
import org.dcache.services.billing.db.data.HitsHourly;
import org.dcache.services.billing.db.data.IPlotData;
import org.dcache.services.billing.db.data.MissesHourly;
import org.dcache.services.billing.db.exceptions.BillingQueryException;
import org.dcache.services.billing.plots.exceptions.TimeFrameFactoryInitializationException;
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
     *
     * @author arossi
     */
    private static class HourlyHitData extends BaseDaily {
        private Long cached = 0L;
        private Long notcached = 0L;

        public Map<String, Double> data() {
            Map<String, Double> dataMap = super.data();
            dataMap.put(HitsDaily.CACHED, cached.doubleValue());
            dataMap.put(HitsDaily.NOT_CACHED, notcached.doubleValue());
            return dataMap;
        }

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

    public void initialize(IBillingInfoAccess access)
                    throws TimeFrameFactoryInitializationException {
        initialize(access, (String) null);
    }

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
                properties.load(new FileInputStream(new File(propertiesPath)));
            }
        } catch (Exception t) {
            throw new TimeFrameFactoryInitializationException(t);
        }
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
    public ITimeFramePlot createPlot(String plotName, String[] subtitles) {
        return new JaidaTimeFramePlot(af, tree, plotName, subtitles, properties);
    }

    @Override
    public ITimeFrameHistogram createDcBytesHistogram(TimeFrame timeFrame,
                    boolean write) throws BillingQueryException {
        String title = write ? getProperty(ITimeFramePlot.LABEL_DC_WR)
                        : getProperty(ITimeFramePlot.LABEL_DC_RD);
        ITimeFrameHistogram histogram = new JaidaTimeFrameHistogram(factory,
                        timeFrame, title);
        histogram.setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
        histogram.setScaling(getProperty(ITimeFramePlot.SCALE_BYTES));
        histogram.setColor(getProperty(ITimeFramePlot.COLOR_DC));
        Collection<IPlotData> plotData = null;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_GBYTES_HR));
            if (write) {
                plotData = getViewData(DcacheWritesHourly.class);
                histogram.setData(plotData, DcacheWritesHourly.SIZE, GB);
            } else {
                plotData = getViewData(DcacheReadsHourly.class);
                histogram.setData(plotData, DcacheReadsHourly.SIZE, GB);
            }
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_GBYTES_DY));
            if (write) {
                plotData = getCoarseGrainedPlotData(DcacheWritesDaily.class,
                                                    timeFrame);
                histogram.setData(plotData, DcacheWritesDaily.SIZE, GB);
            } else {
                plotData = getCoarseGrainedPlotData(DcacheReadsDaily.class,
                                                    timeFrame);
                histogram.setData(plotData, DcacheReadsDaily.SIZE, GB);
            }
        }
        return histogram;
    }

    @Override
    public ITimeFrameHistogram createDcTransfersHistogram(TimeFrame timeFrame,
                    boolean write) throws BillingQueryException {
        String title = write ? getProperty(ITimeFramePlot.LABEL_DC_WR)
                        : getProperty(ITimeFramePlot.LABEL_DC_RD);
        ITimeFrameHistogram histogram = new JaidaTimeFrameHistogram(factory,
                        timeFrame, title);
        histogram.setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
        histogram.setScaling(getProperty(ITimeFramePlot.SCALE_COUNT));
        histogram.setColor(getProperty(ITimeFramePlot.COLOR_DC));
        Collection<IPlotData> plotData = null;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_TRANSF_HR));
            if (write) {
                plotData = getViewData(DcacheWritesHourly.class);
                histogram.setData(plotData, DcacheWritesHourly.COUNT, null);
            } else {
                plotData = getViewData(DcacheReadsHourly.class);
                histogram.setData(plotData, DcacheReadsHourly.COUNT, null);
            }
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_TRANSF_DY));
            if (write) {
                plotData = getCoarseGrainedPlotData(DcacheWritesDaily.class,
                                                    timeFrame);
                histogram.setData(plotData, DcacheWritesDaily.COUNT, null);
            } else {
                plotData = getCoarseGrainedPlotData(DcacheReadsDaily.class,
                                                    timeFrame);
                histogram.setData(plotData, DcacheReadsDaily.COUNT, null);
            }
        }
        return histogram;
    }

    @Override
    public ITimeFrameHistogram createHsmBytesHistogram(TimeFrame timeFrame,
                    boolean write) throws BillingQueryException {
        String title = write ? getProperty(ITimeFramePlot.LABEL_HSM_WR)
                        : getProperty(ITimeFramePlot.LABEL_HSM_RD);
        ITimeFrameHistogram histogram = new JaidaTimeFrameHistogram(factory,
                        timeFrame, title);
        histogram.setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
        histogram.setScaling(getProperty(ITimeFramePlot.SCALE_BYTES));
        histogram.setColor(getProperty(ITimeFramePlot.COLOR_HSM));
        Collection<IPlotData> plotData = null;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_GBYTES_HR));
            if (write) {
                plotData = getViewData(HSMWritesHourly.class);
                histogram.setData(plotData, HSMWritesHourly.SIZE, GB);
            } else {
                plotData = getViewData(HSMReadsHourly.class);
                histogram.setData(plotData, HSMReadsHourly.SIZE, GB);
            }
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_GBYTES_DY));
            if (write) {
                plotData = getCoarseGrainedPlotData(HSMWritesDaily.class,
                                                    timeFrame);
                histogram.setData(plotData, HSMWritesDaily.SIZE, GB);
            } else {
                plotData = getCoarseGrainedPlotData(HSMReadsDaily.class,
                                                    timeFrame);
                histogram.setData(plotData, HSMReadsDaily.SIZE, GB);
            }
        }
        return histogram;
    }

    @Override
    public ITimeFrameHistogram createHsmTransfersHistogram(TimeFrame timeFrame,
                    boolean write) throws BillingQueryException {
        String title = write ? getProperty(ITimeFramePlot.LABEL_HSM_WR)
                        : getProperty(ITimeFramePlot.LABEL_HSM_RD);
        ITimeFrameHistogram histogram = new JaidaTimeFrameHistogram(factory,
                        timeFrame, title);
        histogram.setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
        histogram.setScaling(getProperty(ITimeFramePlot.SCALE_COUNT));
        histogram.setColor(getProperty(ITimeFramePlot.COLOR_HSM));
        Collection<IPlotData> plotData = null;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_TRANSF_HR));
            if (write) {
                plotData = getViewData(HSMWritesHourly.class);
                histogram.setData(plotData, HSMWritesHourly.COUNT, null);
            } else {
                plotData = getViewData(HSMReadsHourly.class);
                histogram.setData(plotData, HSMReadsHourly.COUNT, null);
            }
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_TRANSF_DY));
            if (write) {
                plotData = getCoarseGrainedPlotData(HSMWritesDaily.class,
                                                    timeFrame);
                histogram.setData(plotData, HSMWritesDaily.COUNT, null);
            } else {
                plotData = getCoarseGrainedPlotData(HSMReadsDaily.class,
                                                    timeFrame);
                histogram.setData(plotData, HSMReadsDaily.COUNT, null);
            }
        }
        return histogram;
    }

    @Override
    public ITimeFrameHistogram[] createDcConnectTimeHistograms(
                    TimeFrame timeFrame) throws BillingQueryException {
        Collection<IPlotData> plotData = null;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = getViewData(DcacheTimeHourly.class);
        } else {
            plotData = getCoarseGrainedPlotData(DcacheTimeDaily.class,
                            timeFrame);
        }

        String[] title = new String[] { getProperty(ITimeFramePlot.LABEL_MIN),
                        getProperty(ITimeFramePlot.LABEL_MAX),
                        getProperty(ITimeFramePlot.LABEL_AVG) };
        String[] field = new String[] { DcacheTimeDaily.MIN_TIME,
                        DcacheTimeDaily.MAX_TIME, DcacheTimeDaily.AVG_TIME };
        String[] color = new String[] { getProperty(ITimeFramePlot.COLOR_MIN),
                        getProperty(ITimeFramePlot.COLOR_MAX),
                        getProperty(ITimeFramePlot.COLOR_AVG) };

        ITimeFrameHistogram[] histogram = new ITimeFrameHistogram[3];

        for (int h = 0; h < histogram.length; h++) {
            histogram[h] = new JaidaTimeFrameHistogram(factory, timeFrame,
                            title[h]);
            histogram[h].setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
            histogram[h].setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_CONNECT));
            histogram[h].setColor(color[h]);
            histogram[h].setScaling(getProperty(ITimeFramePlot.SCALE_TIME));
            histogram[h].setData(plotData, field[h],
                            1.0 * TimeUnit.SECONDS.toMillis(1));
        }
        return histogram;
    }

    @Override
    public ITimeFrameHistogram createCostHistogram(TimeFrame timeFrame)
                    throws BillingQueryException {
        ITimeFrameHistogram histogram = new JaidaTimeFrameHistogram(factory,
                        timeFrame, getProperty(ITimeFramePlot.LABEL_COST));
        histogram.setXLabel(getProperty(ITimeFramePlot.LABEL_X_AXIS));
        histogram.setColor(getProperty(ITimeFramePlot.COLOR_DC));
        String scaling = getProperty(ITimeFramePlot.SCALE_COST);
        if (scaling != null)
            histogram.setScaling(scaling);
        Collection<IPlotData> plotData = null;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_COST_HR));
            /*
             * COST HAS NEVER BEEN ACTIVATED AND IS REMOVED IN 2.4+
             */
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_COST_DY));
            plotData = getCoarseGrainedPlotData(CostDaily.class, timeFrame);
            histogram.setData(plotData, CostDaily.TOTAL_COST, null);
        }
        return histogram;
    }

    @Override
    public ITimeFrameHistogram[] createHitHistograms(TimeFrame timeFrame)
                    throws BillingQueryException {
        Collection<IPlotData> plotData = null;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = getHourlyAggregateForHits(timeFrame);
        } else {
            plotData = getCoarseGrainedPlotData(HitsDaily.class, timeFrame);
        }
        ITimeFrameHistogram[] histogram = new ITimeFrameHistogram[2];
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

    private Collection<IPlotData> getHourlyAggregateForHits(TimeFrame timeFrame)
                    throws BillingQueryException {
        Map<String, HourlyHitData> hourlyAggregate = new TreeMap<String, HourlyHitData>();
        Collection<IPlotData> plotData = getViewData(HitsHourly.class);
        for (IPlotData d : plotData) {
            Date date = normalizeForHour(d.timestamp());
            String key = date.toString();
            HourlyHitData hourlyData = hourlyAggregate.get(key);
            if (hourlyData == null) {
                hourlyData = new HourlyHitData();
                hourlyData.setDate(date);
                hourlyAggregate.put(key, hourlyData);
            }
            long count = d.data().get(HitsHourly.COUNT).longValue();
            hourlyData.cached += count;
        }
        plotData = getViewData(MissesHourly.class);
        for (IPlotData d : plotData) {
            Date date = normalizeForHour(d.timestamp());
            String key = date.toString();
            HourlyHitData hourlyData = hourlyAggregate.get(key);
            if (hourlyData == null) {
                hourlyData = new HourlyHitData();
                hourlyData.setDate(date);
                hourlyAggregate.put(key, hourlyData);
            }
            long count = d.data().get(MissesHourly.COUNT).longValue();
            hourlyData.notcached += count;
        }
        plotData = new ArrayList<IPlotData>();
        plotData.addAll(hourlyAggregate.values());
        return plotData;
    }

    /**
     * Rounds down to beginning of the hour in which the timestamp is bounded.
     *
     * @param date
     * @return normalized date (hh:00:00.000)
     */
    private Date normalizeForHour(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    /**
     * @param name
     * @return property value, or <code>null</code> if undefined.
     */
    private String getProperty(String name) {
        return properties.getProperty(name, null);
    }
}
