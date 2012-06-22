package org.dcache.services.billing.plots.jaida;

import hep.aida.IHistogram1D;
import hep.aida.IHistogramFactory;

import java.util.Collection;

import org.dcache.services.billing.db.data.IPlotData;
import org.dcache.services.billing.plots.util.AbstractTimeFrameHistogram;
import org.dcache.services.billing.plots.util.TimeFrame;

/**
 * Wraps IHistogram1D.
 *
 * @see IHistogram1D
 * @author arossi
 */
public final class JaidaTimeFrameHistogram extends AbstractTimeFrameHistogram {

    private final IHistogram1D histogram;

    public JaidaTimeFrameHistogram(IHistogramFactory factory,
                    TimeFrame timeframe, String title) {
        super(timeframe, title);
        double del = timeframe.getBinWidth() / 2.0;
        histogram = factory.createHistogram1D(title, timeframe.getBinCount(),
                        (timeframe.getLowTime().doubleValue() / 1000) - del,
                        (timeframe.getHighTime().doubleValue() / 1000) - del);
    }

    @Override
    public void setData(Collection<IPlotData> data, String field, Double dfactor) {
        if (field != null) {
            if (dfactor != null) {
                for (IPlotData d : data) {
                    histogram.fill(d.timestamp().getTime() / 1000.0, d
                                    .data().get(field) / dfactor);
                }
            } else {
                for (IPlotData d : data) {
                    histogram.fill(d.timestamp().getTime() / 1000.0, d
                                    .data().get(field));
                }
            }
        } else {
            for (IPlotData d : data) {
                histogram.fill(d.timestamp().getTime() / 1000.0);
            }
        }
    }

    public IHistogram1D getHistogram() {
        return histogram;
    }
}
