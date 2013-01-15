package org.dcache.services.billing.plots.util;

import java.util.ArrayList;
import java.util.Collection;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.IPlotData;
import org.dcache.services.billing.db.exceptions.BillingQueryException;
import org.dcache.services.billing.plots.exceptions.TimeFrameFactoryInitializationException;
import org.dcache.services.billing.plots.exceptions.TimeFramePlotException;

/**
 * Base class for ITimeFrameHistogramFactory implementations. <br>
 * <br>
 *
 * Defines protected generic methods for retrieving data.
 *
 * @see ITimeFrameHistogramFactory
 * @author arossi
 */
public abstract class AbstractTimeFrameHistogramFactory implements
                ITimeFrameHistogramFactory {

    protected IBillingInfoAccess access;

    @Override
    public void initialize(IBillingInfoAccess access)
                    throws TimeFrameFactoryInitializationException {
        this.access = access;
    }

    protected <T extends IPlotData> Collection<IPlotData> getCoarseGrainedPlotData(
                    Class<T> clzz, TimeFrame timeFrame)
                    throws TimeFramePlotException {
        return getPlotData(clzz, "date >= date1 && date <= date2",
                        "java.util.Date date1, java.util.Date date2",
                        timeFrame.getLow(), timeFrame.getHigh());
    }

    protected <T extends IPlotData> Collection<IPlotData> getViewData(
                    Class<T> clzz) throws TimeFramePlotException {
        try {
            Collection<T> c = (Collection<T>) access.get(clzz);
            Collection<IPlotData> plotData = new ArrayList<IPlotData>();
            plotData.addAll(c);
            return plotData;
        } catch (BillingQueryException t) {
            throw new TimeFramePlotException(t);
        }
    }

    private <T extends IPlotData> Collection<IPlotData> getPlotData(
                    Class<T> clzz, String filter, String params,
                    Object... values) throws TimeFramePlotException {
        Collection<T> c;
        try {
            c = access.get(clzz, filter, params, values);
        } catch (BillingQueryException t) {
            throw new TimeFramePlotException(t.getMessage(), t);
        }
        Collection<IPlotData> plotData = new ArrayList<>();
        plotData.addAll(c);
        return plotData;
    }
}
