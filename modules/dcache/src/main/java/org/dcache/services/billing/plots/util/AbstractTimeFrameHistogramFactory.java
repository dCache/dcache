package org.dcache.services.billing.plots.util;

import java.util.ArrayList;
import java.util.Collection;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.IPlotData;
import org.dcache.services.billing.db.exceptions.BillingQueryException;
import org.dcache.services.billing.plots.exceptions.TimeFrameFactoryInitializationException;

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

    public void initialize(IBillingInfoAccess access)
                    throws TimeFrameFactoryInitializationException {
        this.access = access;
    }

    protected <T extends IPlotData> Collection<IPlotData> getCoarseGrainedPlotData(
                    Class<T> clzz, TimeFrame timeFrame)
                                    throws BillingQueryException {
        return getPlotData(clzz, "date >= date1 && date <= date2",
                        "java.util.Date date1, java.util.Date date2",
                        timeFrame.getLow(), timeFrame.getHigh());
    }

    protected <T extends IPlotData> Collection<IPlotData> getViewData(Class<T> clzz)
                                    throws BillingQueryException {
        Collection<T> c = (Collection<T>) access.get(clzz);
        Collection<IPlotData> plotData = new ArrayList<IPlotData>();
        plotData.addAll(c);
        return plotData;
    }

    private <T extends IPlotData> Collection<IPlotData> getPlotData(
                    Class<T> clzz, String filter, String params,
                    Object... values) throws BillingQueryException {
        Collection<T> c = (Collection<T>) access.get(clzz, filter, params,
                        values);
        Collection<IPlotData> plotData = new ArrayList<IPlotData>();
        plotData.addAll(c);
        return plotData;
    }
}
