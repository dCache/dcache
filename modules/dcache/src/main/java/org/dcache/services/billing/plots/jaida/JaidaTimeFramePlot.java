package org.dcache.services.billing.plots.jaida;

import hep.aida.IAnalysisFactory;
import hep.aida.IPlotter;
import hep.aida.IPlotterFactory;
import hep.aida.IPlotterStyle;
import hep.aida.ITree;
import hep.aida.ref.plotter.PlotterUtilities;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.dcache.services.billing.plots.util.AbstractTimeFramePlot;
import org.dcache.services.billing.plots.util.ITimeFrameHistogram;
import org.dcache.services.billing.plots.util.PlotGridPosition;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Wraps IPlotterFactory and IPlotter.
 *
 * @see IPlotterFactory
 * @see IPlotter
 * @author arossi
 */
public final class JaidaTimeFramePlot extends AbstractTimeFramePlot {
    private static final Logger _log = LoggerFactory.getLogger(JaidaTimeFramePlot.class);
    private final IPlotterFactory factory;
    private final IPlotter plotter;
    private List<IPlotterStyle> styles;
    private String[] titles;

    /**
     * @param af
     * @param tree
     * @param plotName
     * @param titles
     * @param properties
     */
    public JaidaTimeFramePlot(IAnalysisFactory af, ITree tree, String plotName,
                    String[] titles, Properties properties) {
        super(properties);
        this.name = plotName;
        factory = af.createPlotterFactory();
        plotter = factory.create(plotName);
        if (titles == null) {
            this.titles = new String[0];
        } else {
            this.titles = Arrays.copyOf(titles, titles.length);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.statistics.util.ITimeFramePlot#plot()
     */
    @Override
    public void plot() {
        setHistogramStyles();
        plotter.createRegions(rows, cols);
        int region = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                PlotGridPosition p = new PlotGridPosition(row, col);
                List<ITimeFrameHistogram> histograms = getHistogramsForPosition(p);
                for (ITimeFrameHistogram h : histograms) {
                    IPlotterStyle style = factory.createPlotterStyle();
                    normalizeTitleStyle(style);
                    normalizeDataStyleForConnected(style, h);
                    normalizeXAxisStyle(style, h);
                    normalizeYAxisStyle(style, h);
                    if (region < titles.length) {
                        plotter.region(region).setTitle(titles[region]);
                    }
                    plotter.region(region)
                    .plot(((JaidaTimeFrameHistogram) h)
                                    .getHistogram(),
                                    style);
                }
                region++;
            }
        }

        if (exportSubdir != null) {
            exportPlot();
        } else {
            plotter.show();
        }
    }

    /**
     * create image file
     */
    private void exportPlot() {
        File path = new File(exportSubdir, name + extension);
        try {
            PlotterUtilities.writeToFile(plotter, path.getAbsolutePath(),
                            imageType, properties);
        } catch (IOException e) {
            _log.error("Cannot write billing plot: " + e.getMessage());
        }
    }

    /**
     * Generates the histograms and sets their styles.
     */
    private void setHistogramStyles() {
        styles = new ArrayList<IPlotterStyle>();
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                PlotGridPosition p = new PlotGridPosition(row, col);
                List<ITimeFrameHistogram> histograms = getHistogramsForPosition(p);
                for (ITimeFrameHistogram h : histograms) {
                    IPlotterStyle style = factory.createPlotterStyle();
                    normalizeTitleStyle(style);
                    normalizeDataStyleForConnected(style, h);
                    normalizeXAxisStyle(style, h);
                    normalizeYAxisStyle(style, h);
                    styles.add(style);
                }
            }
        }
    }

    /**
     * Sets title from properties.
     *
     * @param style
     */
    private void normalizeTitleStyle(IPlotterStyle style) {
        style.titleStyle()
        .textStyle()
        .setFontSize(Integer.parseInt(properties
                        .getProperty(PLOT_TITLE_SIZE)));
        style.titleStyle().textStyle().setBold(true);
        style.titleStyle().textStyle()
        .setColor(properties.getProperty(PLOT_TITLE_COLOR));
        style.titleStyle().textStyle().setVisible(true);
    }

    /**
     * Sets data style from properties.
     *
     * @param histogramStyle
     * @param histogram
     */
    private void normalizeDataStyleForConnected(IPlotterStyle histogramStyle,
                    ITimeFrameHistogram histogram) {
        histogramStyle.dataStyle().errorBarStyle().setVisible(false);
        histogramStyle.dataStyle().fillStyle().setVisible(false);
        histogramStyle.dataStyle().lineStyle().setVisible(false);
        histogramStyle.dataStyle().showInStatisticsBox(false);

        histogramStyle.dataStyle().outlineStyle().setVisible(true);
        histogramStyle.dataStyle().outlineStyle()
        .setColor(histogram.getColor());
        histogramStyle.dataStyle()
        .outlineStyle()
        .setThickness(Integer.parseInt(properties
                        .getProperty(CURVE_THICKNESS)));
        histogramStyle.dataStyle().markerStyle()
        .setShape(properties.getProperty(MARKER_SHAPE));
        histogramStyle.dataStyle()
        .markerStyle()
        .setSize(Integer.parseInt(properties
                        .getProperty(MARKER_SIZE)));
        histogramStyle.dataStyle().markerStyle().setColor(histogram.getColor());
        histogramStyle.dataStyle().markerStyle().setVisible(true);
    }

    /**
     * Sets x-axis style from properties.
     *
     * @param histogramStyle
     * @param histogram
     */
    private void normalizeXAxisStyle(IPlotterStyle histogramStyle,
                    ITimeFrameHistogram histogram) {
        histogramStyle.xAxisStyle().setParameter("type",
                        properties.getProperty(X_AXIS_TYPE));
        histogramStyle.xAxisStyle().setLabel(histogram.getXLabel());
        histogramStyle.xAxisStyle().labelStyle().setBold(true);
        histogramStyle.xAxisStyle()
        .labelStyle()
        .setFontSize(Integer.parseInt(properties
                        .getProperty(X_AXIS_SIZE)));
        histogramStyle.xAxisStyle().labelStyle().setItalic(true);
        histogramStyle.xAxisStyle().labelStyle()
        .setColor(properties.getProperty(X_AXIS_LABEL_COLOR));
        histogramStyle.xAxisStyle()
        .tickLabelStyle()
        .setFontSize(Integer.parseInt(properties
                        .getProperty(X_AXIS_TICK_SIZE)));
        histogramStyle.xAxisStyle().tickLabelStyle().setBold(true);
        histogramStyle.xAxisStyle()
        .tickLabelStyle()
        .setColor(properties
                        .getProperty(X_AXIS_TICK_LABEL_COLOR));
    }

    /**
     * Sets y-axis style from properties.
     *
     * @param histogramStyle
     * @param histogram
     */
    private void normalizeYAxisStyle(IPlotterStyle histogramStyle,
                    ITimeFrameHistogram histogram) {
        histogramStyle.yAxisStyle().setLabel(histogram.getYLabel());
        histogramStyle.yAxisStyle().labelStyle().setBold(true);
        histogramStyle.yAxisStyle()
        .labelStyle()
        .setFontSize(Integer.parseInt(properties
                        .getProperty(Y_AXIS_SIZE)));
        histogramStyle.yAxisStyle().labelStyle().setItalic(true);
        histogramStyle.yAxisStyle().labelStyle()
        .setColor(properties.getProperty(Y_AXIS_LABEL_COLOR));
        histogramStyle.yAxisStyle()
        .tickLabelStyle()
        .setFontSize(Integer.parseInt(properties
                        .getProperty(Y_AXIS_TICK_SIZE)));
        histogramStyle.yAxisStyle().tickLabelStyle().setBold(true);
        histogramStyle.yAxisStyle()
        .tickLabelStyle()
        .setColor(properties
                        .getProperty(Y_AXIS_TICK_LABEL_COLOR));
        histogramStyle.yAxisStyle().setScaling(histogram.getScaling());
        histogramStyle.yAxisStyle().setParameter("allowZeroSuppression",
                        properties.getProperty(Y_AXIS_ALLOW_ZERO_SUPPRESSION));
    }
}
