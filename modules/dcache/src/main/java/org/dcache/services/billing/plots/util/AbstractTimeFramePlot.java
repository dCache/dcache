package org.dcache.services.billing.plots.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Base class for ITimeFramePlot implementations. Handles basic properties
 * and histogram mapping.
 *
 * @see ITimeFramePlot
 * @author arossi
 */
public abstract class AbstractTimeFramePlot implements ITimeFramePlot {

    protected final Properties properties;
    protected int rows;
    protected int cols;
    protected String name;
    protected File exportSubdir;
    protected String extension;
    protected String imageType;
    protected final Map<String, List<ITimeFrameHistogram>> histograms;

    /**
     * Sets base properties
     */
    protected AbstractTimeFramePlot(Properties properties) {
        this.properties = properties;
        rows = Integer.parseInt(properties
                        .getProperty(ITimeFramePlot.PLOT_GRID_ROWS));
        cols = Integer.parseInt(properties
                        .getProperty(ITimeFramePlot.PLOT_GRID_COLS));
        String d = properties.getProperty(ITimeFramePlot.EXPORT_SUBDIR);
        if (d != null) {
            exportSubdir = new File(d);
            imageType = properties.getProperty(ITimeFramePlot.EXPORT_TYPE);
            extension = properties.getProperty(ITimeFramePlot.EXPORT_EXTENSION);
        }
        histograms
            = Collections.synchronizedMap(new HashMap<String,
                                            List<ITimeFrameHistogram>>());
    }

    public void addHistogram(PlotGridPosition position,
                    ITimeFrameHistogram histogram) {
        List<ITimeFrameHistogram> current = getHistogramsForPosition(position);
        current.add(histogram);
    }

    public List<ITimeFrameHistogram> getHistogramsForPosition(
                    PlotGridPosition position) {
        checkPosition(position);
        List<ITimeFrameHistogram> current = histograms.get(position.getKey());
        if (current == null) {
            current = new ArrayList<ITimeFrameHistogram>();
            histograms.put(position.getKey(), current);
        }
        return current;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Checks position to see if legitimate.
     */
    private void checkPosition(PlotGridPosition position)
                    throws IllegalArgumentException {
        if (position == null)
            throw new IllegalArgumentException("no grid position defined");
        int row = position.getRow();
        int col = position.getCol();
        if (row < 0 || row >= rows || col < 0 || col >= cols)
            throw new IllegalArgumentException("position " + position.getKey()
                            + " not on defined grid " + rows + " X " + cols);
    }

    /**
     * @return the properties
     */
    public Properties getProperties() {
        return properties;
    }
}
