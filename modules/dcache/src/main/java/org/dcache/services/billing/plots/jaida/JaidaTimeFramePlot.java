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
package org.dcache.services.billing.plots.jaida;

import hep.aida.IAnalysisFactory;
import hep.aida.IHistogram1D;
import hep.aida.IPlotter;
import hep.aida.IPlotterFactory;
import hep.aida.IPlotterStyle;
import hep.aida.ITree;
import hep.aida.ref.plotter.PlotterUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.dcache.services.billing.histograms.config.HistogramWrapper;
import org.dcache.services.billing.plots.util.AbstractTimeFramePlot;
import org.dcache.services.billing.plots.util.TimeFramePlotProperties;
import org.dcache.services.billing.plots.util.PlotGridPosition;

/**
 * JAIDA-specific plot wrapper. Delegates properties to JAIDA API.
 *
 * @author arossi
 */
public final class JaidaTimeFramePlot extends AbstractTimeFramePlot {
    private static final Logger logger
        = LoggerFactory.getLogger(JaidaTimeFramePlot.class);

    private final IPlotterFactory factory;
    private final IPlotter plotter;
    private final String[] titles;

    public JaidaTimeFramePlot(IAnalysisFactory af, ITree tree, String name,
                    String[] titles, Properties properties) {
        super(name, properties);
        factory = af.createPlotterFactory();
        plotter = factory.create(name);
        if (titles == null) {
            this.titles = new String[0];
        } else {
            this.titles = Arrays.copyOf(titles, titles.length);
        }
    }

    @Override
    public void plot() {
        plotter.createRegions(rows, cols);
        int region = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                PlotGridPosition p = new PlotGridPosition(row, col);
                List<HistogramWrapper<IHistogram1D>> histograms
                    = getHistogramsForPosition(p);
                for (HistogramWrapper<IHistogram1D> h : histograms) {
                    IPlotterStyle style = factory.createPlotterStyle();
                    normalizeTitleStyle(style);
                    normalizeDataStyle(style, h);
                    normalizeXAxisStyle(style, h);
                    normalizeYAxisStyle(style, h);
                    if (region < titles.length) {
                        plotter.region(region).setTitle(titles[region]);
                    }
                    plotter.region(region).plot(h.getHistogram(), style);
                }
                region++;
            }
        }

        try {
            if (exportSubdir != null) {
                exportPlot();
            } else {
                plotter.show();
            }
        } catch (RuntimeException e) {
            /*
             * JAIDA show() actually declares RuntimeException thrown
             * We do not want these exceptions to be fatal, so we
             * only let JVM errors pass here
             */
            logger.error("plotting failed from unexpected error: {}",
                            e.getMessage());
            logger.debug("plot", e);
        }
    }

    private void exportPlot() {
        File path = new File(exportSubdir, name + extension);
        try {
            PlotterUtilities.writeToFile(plotter, path.getAbsolutePath(),
                            imageType, properties);
        } catch (IOException e) {
            logger.error("Cannot write billing plot: {}", e.getMessage());
        }
    }

    private void normalizeDataStyle(IPlotterStyle histogramStyle,
                    HistogramWrapper<IHistogram1D> histogram) {
        histogramStyle.dataStyle().errorBarStyle().setVisible(false);
        histogramStyle.dataStyle().showInStatisticsBox(false);

        String color = histogram.getColor();
        String markerShape
            = properties.getProperty(TimeFramePlotProperties.MARKER_SHAPE);
        Integer curveThickness
            = Integer.parseInt(properties.getProperty(TimeFramePlotProperties.CURVE_THICKNESS));
        Integer markerSize
            = Integer.parseInt(properties.getProperty(TimeFramePlotProperties.MARKER_SIZE));
        Integer outlineThickness
            = Integer.parseInt(properties.getProperty(TimeFramePlotProperties.OUTLINE_THICKNESS));
        Double opacity
            = Double.parseDouble(properties.getProperty(TimeFramePlotProperties.OPACITY));

        switch (histogram.getStyle()) {
            case CONNECTED:
                histogramStyle.dataStyle().fillStyle().setVisible(false);
                histogramStyle.dataStyle().outlineStyle().setVisible(true);
                histogramStyle.dataStyle().outlineStyle().setColor(color);
                histogramStyle.dataStyle().outlineStyle().setThickness(curveThickness);
                histogramStyle.dataStyle().markerStyle().setSize(markerSize);
                histogramStyle.dataStyle().markerStyle().setShape(markerShape);
                histogramStyle.dataStyle().markerStyle().setColor(color);
                histogramStyle.dataStyle().markerStyle().setVisible(true);
                histogramStyle.dataStyle().lineStyle().setVisible(false);
                break;
            case OUTLINE:
                histogramStyle.dataStyle().fillStyle().setVisible(false);
                histogramStyle.dataStyle().outlineStyle().setVisible(false);
                histogramStyle.dataStyle().markerStyle().setVisible(false);
                histogramStyle.dataStyle().lineStyle().setColor(color);
                histogramStyle.dataStyle().lineStyle().setThickness(outlineThickness);
                histogramStyle.dataStyle().lineStyle().setVisible(true);
                break;
            case FILLED:
                histogramStyle.dataStyle().fillStyle().setVisible(true);
                histogramStyle.dataStyle().fillStyle().setColor(color);
                histogramStyle.dataStyle().fillStyle().setOpacity(opacity);
                histogramStyle.dataStyle().outlineStyle().setVisible(false);
                histogramStyle.dataStyle().markerStyle().setVisible(false);
                histogramStyle.dataStyle().lineStyle().setColor(color);
                histogramStyle.dataStyle().lineStyle().setVisible(true);
                break;
        }
    }

    private void normalizeTitleStyle(IPlotterStyle style) {
        style.titleStyle().textStyle().setFontSize(
                        Integer.parseInt(properties.getProperty
                                        (TimeFramePlotProperties.PLOT_TITLE_SIZE)));
        style.titleStyle().textStyle().setBold(true);
        style.titleStyle().textStyle().setColor(
                        properties.getProperty(TimeFramePlotProperties.PLOT_TITLE_COLOR));
        style.titleStyle().textStyle().setVisible(true);
    }

    private void normalizeXAxisStyle(IPlotterStyle histogramStyle,
                    HistogramWrapper<IHistogram1D> histogram) {
        histogramStyle.xAxisStyle().setParameter("type",
                        properties.getProperty(TimeFramePlotProperties.X_AXIS_TYPE));
        histogramStyle.xAxisStyle().setLabel(histogram.getXLabel());
        histogramStyle.xAxisStyle().labelStyle().setBold(true);
        histogramStyle.xAxisStyle().labelStyle().setFontSize(
                        Integer.parseInt(properties.getProperty
                                        (TimeFramePlotProperties.X_AXIS_SIZE)));
        histogramStyle.xAxisStyle().labelStyle().setItalic(true);
        histogramStyle.xAxisStyle().labelStyle().setColor(
                        properties.getProperty
                            (TimeFramePlotProperties.X_AXIS_LABEL_COLOR));
        histogramStyle.xAxisStyle().tickLabelStyle().setFontSize(
                        Integer.parseInt(properties.getProperty
                                        (TimeFramePlotProperties.X_AXIS_TICK_SIZE)));
        histogramStyle.xAxisStyle().tickLabelStyle().setBold(true);
        histogramStyle.xAxisStyle().tickLabelStyle().setColor(
                        properties.getProperty
                            (TimeFramePlotProperties.X_AXIS_TICK_LABEL_COLOR));
    }

    private void normalizeYAxisStyle(IPlotterStyle histogramStyle,
                    HistogramWrapper<IHistogram1D> histogram) {
        histogramStyle.yAxisStyle().setLabel(histogram.getYLabel());
        histogramStyle.yAxisStyle().labelStyle().setBold(true);
        histogramStyle.yAxisStyle().labelStyle().setFontSize(
                        Integer.parseInt(properties.getProperty
                                        (TimeFramePlotProperties.Y_AXIS_SIZE)));
        histogramStyle.yAxisStyle().labelStyle().setItalic(true);
        histogramStyle.yAxisStyle().labelStyle().setColor(
                        properties.getProperty
                            (TimeFramePlotProperties.Y_AXIS_LABEL_COLOR));
        histogramStyle.yAxisStyle().tickLabelStyle().setFontSize(
                        Integer.parseInt(properties.getProperty
                                        (TimeFramePlotProperties.Y_AXIS_TICK_SIZE)));
        histogramStyle.yAxisStyle().tickLabelStyle().setBold(true);
        histogramStyle.yAxisStyle().tickLabelStyle().setColor(
                        properties.getProperty
                            (TimeFramePlotProperties.Y_AXIS_TICK_LABEL_COLOR));
        histogramStyle.yAxisStyle().setScaling(histogram.getScaling());
        histogramStyle.yAxisStyle().setParameter("allowZeroSuppression",
                        properties.getProperty
                            (TimeFramePlotProperties.Y_AXIS_ALLOW_ZERO_SUPPRESSION));
    }
}
