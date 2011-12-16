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

    /**
     * @param timeframe
     */
    public JaidaTimeFrameHistogram(IHistogramFactory factory,
                    TimeFrame timeframe, String title) {
        super(timeframe, title);
        double del = timeframe.getBinWidth() / 2.0;
        histogram = factory.createHistogram1D(title, timeframe.getBinCount(),
                        (timeframe.getLowTime().doubleValue() / 1000) - del,
                        (timeframe.getHighTime().doubleValue() / 1000) - del);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.statistics.util.ITimeFrameHistogram#setData()
     */
    @Override
    public void setData(Collection<IPlotData> data, String field, Double dfactor)
                    throws Throwable {
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

    /**
     * @return the histogram
     */
    public IHistogram1D getHistogram() {
        return histogram;
    }
}
