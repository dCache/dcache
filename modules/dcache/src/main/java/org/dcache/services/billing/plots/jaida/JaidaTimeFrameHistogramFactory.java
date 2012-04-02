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
import org.dcache.services.billing.db.data.CostDaily;
import org.dcache.services.billing.db.data.DcacheReadsDaily;
import org.dcache.services.billing.db.data.DcacheTimeDaily;
import org.dcache.services.billing.db.data.DcacheWritesDaily;
import org.dcache.services.billing.db.data.HSMReadsDaily;
import org.dcache.services.billing.db.data.HSMWritesDaily;
import org.dcache.services.billing.db.data.HitsDaily;
import org.dcache.services.billing.db.data.IPlotData;
import org.dcache.services.billing.db.data.MoverData;
import org.dcache.services.billing.db.data.PoolCostData;
import org.dcache.services.billing.db.data.PoolHitData;
import org.dcache.services.billing.db.data.StorageData;
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

        /*
         * (non-Javadoc)
         *
         * @see org.dcache.billing.data.BaseDaily#data()
         */
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

    /**
     * @param access
     */
    public JaidaTimeFrameHistogramFactory() {
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.AbstractTimeFrameHistogramFactory#
     * initialize(org.dcache.billing.IBillingInfoAccess)
     */
    public void initialize(IBillingInfoAccess access)
                    throws TimeFrameFactoryInitializationException {
        initialize(access, (String) null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.ITimeFrameHistogramFactory#initialize
     * (org.dcache.billing.IBillingInfoAccess, java.lang.String)
     */
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
                InputStream stream = new FileInputStream(new File(propertiesPath));
                try {
                    properties.load(stream);
                } finally {
                    stream.close();
                }
            }
        } catch (Throwable t) {
            throw new TimeFrameFactoryInitializationException(t);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.ITimeFrameHistogramFactory#initialize
     * (org.dcache.billing.IBillingInfoAccess, java.util.Properties)
     */
    @Override
    public void initialize(IBillingInfoAccess access, Properties properties)
                    throws TimeFrameFactoryInitializationException {
        super.initialize(access);
        af = IAnalysisFactory.create();
        tree = af.createTreeFactory().create();
        factory = af.createHistogramFactory(tree);
        this.properties = properties;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.ITimeFrameHistogramFactory#createPlot
     * (java.lang.String)
     */
    @Override
    public ITimeFramePlot createPlot(String plotName, String[] subtitles) {
        return new JaidaTimeFramePlot(af, tree, plotName, subtitles, properties);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.services.billing.plots.util.ITimeFrameHistogramFactory#
     * createDcBytesHistogram(org.dcache.services.billing.plots.util.TimeFrame,
     * boolean)
     */
    @Override
    public ITimeFrameHistogram createDcBytesHistogram(TimeFrame timeFrame,
                    boolean write) {
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
            try {
                plotData = getFineGrainedPlotData(MoverData.class, timeFrame,
                                "isNew", "java.lang.Boolean", write);
                histogram.setData(plotData, MoverData.TRANSFER_SIZE, GB);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_GBYTES_DY));
            try {
                if (write) {
                    plotData = getCoarseGrainedPlotData(
                                    DcacheWritesDaily.class, timeFrame);
                    histogram.setData(plotData, DcacheWritesDaily.SIZE, GB);
                } else {
                    plotData = getCoarseGrainedPlotData(DcacheReadsDaily.class,
                                    timeFrame);
                    histogram.setData(plotData, DcacheReadsDaily.SIZE, GB);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        return histogram;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.services.billing.plots.util.ITimeFrameHistogramFactory#
     * createDcTransfersHistogram
     * (org.dcache.services.billing.plots.util.TimeFrame, boolean)
     */
    @Override
    public ITimeFrameHistogram createDcTransfersHistogram(TimeFrame timeFrame,
                    boolean write) throws Throwable {
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
            try {
                plotData = getFineGrainedPlotData(MoverData.class, timeFrame,
                                "isNew", "java.lang.Boolean", write);
                histogram.setData(plotData, null, null);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        } else {
            try {
                histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_TRANSF_DY));
                if (write) {
                    plotData = getCoarseGrainedPlotData(
                                    DcacheWritesDaily.class, timeFrame);
                    histogram.setData(plotData, DcacheWritesDaily.COUNT, null);
                } else {
                    plotData = getCoarseGrainedPlotData(DcacheReadsDaily.class,
                                    timeFrame);
                    histogram.setData(plotData, DcacheReadsDaily.COUNT, null);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        return histogram;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.services.billing.plots.util.ITimeFrameHistogramFactory#
     * createHsmBytesHistogram(org.dcache.services.billing.plots.util.TimeFrame,
     * boolean)
     */
    @Override
    public ITimeFrameHistogram createHsmBytesHistogram(TimeFrame timeFrame,
                    boolean write) {
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
            try {
                plotData = getFineGrainedPlotData(StorageData.class, timeFrame,
                                "action", "java.lang.String", write ? "store"
                                                : "restore");
                histogram.setData(plotData, StorageData.FULL_SIZE, GB);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_GBYTES_DY));
            try {
                if (write) {
                    plotData = getCoarseGrainedPlotData(HSMWritesDaily.class,
                                    timeFrame);
                    histogram.setData(plotData, HSMWritesDaily.SIZE, GB);
                } else {
                    plotData = getCoarseGrainedPlotData(HSMReadsDaily.class,
                                    timeFrame);
                    histogram.setData(plotData, HSMReadsDaily.SIZE, GB);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        return histogram;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.services.billing.plots.util.ITimeFrameHistogramFactory#
     * createHsmTransfersHistogram
     * (org.dcache.services.billing.plots.util.TimeFrame, boolean)
     */
    @Override
    public ITimeFrameHistogram createHsmTransfersHistogram(TimeFrame timeFrame,
                    boolean write) throws Throwable {
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
            try {
                plotData = getFineGrainedPlotData(StorageData.class, timeFrame,
                                "action", "java.lang.String", write ? "store"
                                                : "restore");
                histogram.setData(plotData, null, null);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_TRANSF_DY));
            try {
                if (write) {
                    plotData = getCoarseGrainedPlotData(HSMWritesDaily.class,
                                    timeFrame);
                    histogram.setData(plotData, HSMWritesDaily.COUNT, null);
                } else {
                    plotData = getCoarseGrainedPlotData(HSMReadsDaily.class,
                                    timeFrame);
                    histogram.setData(plotData, HSMReadsDaily.COUNT, null);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        return histogram;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.services.billing.plots.util.ITimeFrameHistogramFactory#
     * createDcConnectTimeHistograms
     * (org.dcache.services.billing.plots.util.TimeFrame)
     */
    @Override
    public ITimeFrameHistogram[] createDcConnectTimeHistograms(
                    TimeFrame timeFrame) throws Throwable {
        Collection<IPlotData> plotData = null;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            try {
                plotData = getHourlyAggregateForTime(timeFrame);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        } else {
            try {
                plotData = getCoarseGrainedPlotData(DcacheTimeDaily.class,
                                timeFrame);
            } catch (Throwable t) {
                t.printStackTrace();
            }
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
                            1.0 * TimeUnit.MINUTES.toMillis(1));
        }
        return histogram;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.services.billing.plots.util.ITimeFrameHistogramFactory#
     * createCostHistogram(org.dcache.services.billing.plots.util.TimeFrame)
     */
    @Override
    public ITimeFrameHistogram createCostHistogram(TimeFrame timeFrame) {
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
            try {
                plotData = getFineGrainedPlotData(PoolCostData.class, timeFrame);
                histogram.setData(plotData, PoolCostData.COST, null);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        } else {
            histogram.setYLabel(getProperty(ITimeFramePlot.LABEL_Y_AXIS_COST_DY));
            try {
                plotData = getCoarseGrainedPlotData(CostDaily.class, timeFrame);
                histogram.setData(plotData, CostDaily.TOTAL_COST, null);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        return histogram;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.services.billing.plots.util.ITimeFrameHistogramFactory#
     * createHitHistograms(org.dcache.services.billing.plots.util.TimeFrame)
     */
    @Override
    public ITimeFrameHistogram[] createHitHistograms(TimeFrame timeFrame)
                    throws Throwable {
        Collection<IPlotData> plotData = null;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            try {
                plotData = getHourlyAggregateForHits(timeFrame);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        } else {
            try {
                plotData = getCoarseGrainedPlotData(HitsDaily.class, timeFrame);
            } catch (Throwable t) {
                t.printStackTrace();
            }
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

    /**
     * Does hourly aggregation of a bounded set of billing info data points in
     * memory.
     *
     * @param timeFrame
     * @param field
     * @return data points
     * @throws BillingQueryException
     */
    private Collection<IPlotData> getHourlyAggregateForTime(TimeFrame timeFrame)
                    throws BillingQueryException {
        Collection<IPlotData> plotData = getFineGrainedPlotData(
                        MoverData.class, timeFrame);
        Map<String, DcacheTimeDaily> hourlyAggregate = new TreeMap<String, DcacheTimeDaily>();

        for (IPlotData d : plotData) {
            Date date = normalizeForHour(d.timestamp());
            String key = date.toString();
            DcacheTimeDaily hourlyData = hourlyAggregate.get(key);
            if (hourlyData == null) {
                hourlyData = new DcacheTimeDaily();
                hourlyData.setDate(date);
                hourlyAggregate.put(key, hourlyData);
            }
            long time = d.data().get(MoverData.CONNECTION_TIME).longValue();
            hourlyData.setCount(hourlyData.getCount() + 1);
            hourlyData.setTotalTime(hourlyData.getTotalTime() + time);
            long min = hourlyData.getMinimum();
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
     * @param timeFrame
     * @return data points
     * @throws BillingQueryException
     */
    private Collection<IPlotData> getHourlyAggregateForHits(TimeFrame timeFrame)
                    throws BillingQueryException {
        Collection<IPlotData> plotData = getFineGrainedPlotData(
                        PoolHitData.class, timeFrame);
        Map<String, HourlyHitData> hourlyAggregate = new TreeMap<String, HourlyHitData>();
        for (IPlotData d : plotData) {
            boolean cached = ((PoolHitData) d).getFileCached();
            Date date = normalizeForHour(d.timestamp());
            String key = date.toString();
            HourlyHitData hourlyData = hourlyAggregate.get(key);
            if (hourlyData == null) {
                hourlyData = new HourlyHitData();
                hourlyData.setDate(date);
                hourlyAggregate.put(key, hourlyData);
            }
            if (cached)
                hourlyData.cached++;
            else
                hourlyData.notcached++;
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
