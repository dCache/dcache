package org.dcache.services.billing.plots.util;

import java.util.List;

/**
 * Defines a plot based on the {@link TimeFrame} abstraction, containing an
 * abitrary number of 1-D time histograms.
 *
 * @author arossi
 */
public interface ITimeFramePlot {

    /**
     * The following constants correspond to the names of the properties which
     * can be defined in <code>plot.properties</code>.
     */

    /**
     * Interval between plot generations.
     */
    final String THREAD_TIMEOUT_TYPE = "thread.timeout.in.minutes";

    /**
     * Implementation type (currently only
     * {@link org.dcache.services.billing.plots.jaida.JaidaTimeFrameHistogramFactory}
     * is available).
     */
    final String FACTORY_TYPE = "histogram.factory";

    final String PLOT_TITLE_COLOR = "plot.title.color";
    final String PLOT_TITLE_SIZE = "plot.title.size";

    /**
     * Number of rows and columns for the plot grid; e.g., (2,2) would define a
     * plot divided into quadrants, each displying its own histograms. (1,1)
     * means the plot has only a single histogram area.
     */
    final String PLOT_GRID_ROWS = "plot.grid.rows";
    final String PLOT_GRID_COLS = "plot.grid.cols";

    final String PLOT_WIDTH = "plotWidth";
    final String PLOT_HEIGHT = "plotHeight";
    final String CURVE_THICKNESS = "curve.thickness";
    final String MARKER_SHAPE = "marker.shape";
    final String MARKER_SIZE = "marker.size";
    final String X_AXIS_TYPE = "x.axis.type";
    final String X_AXIS_SIZE = "x.axis.size";
    final String X_AXIS_LABEL_COLOR = "x.axis.label.color";
    final String X_AXIS_TICK_SIZE = "x.axis.tick.size";
    final String X_AXIS_TICK_LABEL_COLOR = "x.axis.tick.label.color";
    final String Y_AXIS_SIZE = "y.axis.size";
    final String Y_AXIS_LABEL_COLOR = "y.axis.label.color";
    final String Y_AXIS_TICK_SIZE = "y.axis.tick.size";
    final String Y_AXIS_TICK_LABEL_COLOR = "y.axis.tick.label.color";
    final String Y_AXIS_ALLOW_ZERO_SUPPRESSION = "y.axis.allow.zero.suppression";

    /**
     * Where the plot image files are written
     */
    final String EXPORT_SUBDIR = "export.subdir";

    /**
     * e.g., gif, png, etc.
     */
    final String EXPORT_TYPE = "export.imagetype";

    /**
     * e.g., .gif, .png, etc.
     */
    final String EXPORT_EXTENSION = "export.extension";

    /**
     * Titles for the plot types.
     */
    final String TITLE_BYTES_RD = "title.bytes.read";
    final String TITLE_BYTES_WR = "title.bytes.written";
    final String TITLE_TRANSF_RD = "title.transfers.read";
    final String TITLE_TRANSF_WR = "title.transfers.write";
    final String TITLE_CONN_TM = "title.connection.time";
    final String TITLE_CACHE_HITS = "title.cache.hits";
    final String TITLE_POOL_COST = "title.pool.cost";

    /**
     * Descriptive banner for the time-category of the plot.
     */
    final String DESC_DAILY = "desc.daily";
    final String DESC_WEEKLY = "desc.weekly";
    final String DESC_MONTHLY = "desc.monthly";
    final String DESC_YEARLY = "desc.yearly";

    /**
     * Labels for axes according to plot type and histogram time-binning.
     */
    final String LABEL_X_AXIS = "label.x.axis";
    final String LABEL_Y_AXIS_GBYTES_HR = "label.y.axis.gbytes.hourly";
    final String LABEL_Y_AXIS_GBYTES_DY = "label.y.axis.gbytes.daily";
    final String LABEL_Y_AXIS_TRANSF_HR = "label.y.axis.transfers.hourly";
    final String LABEL_Y_AXIS_TRANSF_DY = "label.y.axis.transfers.daily";
    final String LABEL_Y_AXIS_CONNECT = "label.y.axis.connection.time";
    final String LABEL_Y_AXIS_HITS_HR = "label.y.axis.hits.hourly";
    final String LABEL_Y_AXIS_HITS_DY = "label.y.axis.hits.daily";
    final String LABEL_Y_AXIS_COST_HR = "label.y.axis.cost.hourly";
    final String LABEL_Y_AXIS_COST_DY = "label.y.axis.cost.daily";

    /**
     * Histogram labels for plot legend.
     */
    final String LABEL_DC_RD = "label.dcache.reads";
    final String LABEL_DC_WR = "label.dcache.writes";
    final String LABEL_HSM_RD = "label.hsm.reads";
    final String LABEL_HSM_WR = "label.hsm.writes";
    final String LABEL_MIN = "label.minimum";
    final String LABEL_MAX = "label.maximum";
    final String LABEL_AVG = "label.average";
    final String LABEL_COST = "label.cost";
    final String LABEL_CACHED = "label.cached";
    final String LABEL_NCACHED = "label.ncached";

    /**
     * Histogram colors.
     */
    final String COLOR_DC = "color.dcache";
    final String COLOR_HSM = "color.hsm";
    final String COLOR_MAX = "color.max";
    final String COLOR_MIN = "color.min";
    final String COLOR_AVG = "color.avg";
    final String COLOR_CACHED = "color.cached";
    final String COLOR_NCACHED = "color.ncached";

    /**
     * Y-axis scaling according to histogram types.
     */
    final String SCALE_BYTES = "scaling.bytes";
    final String SCALE_COUNT = "scaling.count";
    final String SCALE_TIME = "scaling.time";
    final String SCALE_COST = "scaling.cost";

    void setName(String name);

    /**
     * Add a @see ITimeFrameHistogram to the plot.
     *
     * @param position
     *            (row, col) on 2-D grid.
     */
    void addHistogram(PlotGridPosition position, ITimeFrameHistogram histogram);

    /**
     * @param position
     *            (row, col) on 2-D grid.
     */
    List<ITimeFrameHistogram> getHistogramsForPosition(PlotGridPosition position);

    /**
     * implementation-specific callout
     */
    void plot();
}
