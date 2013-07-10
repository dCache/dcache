package org.dcache.services.billing.plots.util;

import java.util.List;

import org.dcache.services.billing.histograms.TimeFrame;
import org.dcache.services.billing.histograms.config.HistogramWrapper;

/**
 * Defines a plot based on the {@link TimeFrame} abstraction, containing an
 * abitrary number of 1-D time histograms.
 *
 * @author arossi
 */
public interface ITimeFramePlot<H> {

    void addHistogram(PlotGridPosition position, HistogramWrapper<H> config);

    List<HistogramWrapper<H>> getHistogramsForPosition(PlotGridPosition position);

    void plot();
}
