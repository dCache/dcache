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

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.ITimeFrameHistogramFactory#initialize
     * (org.dcache.billing.IBillingInfoAccess)
     */
    public void initialize(IBillingInfoAccess access)
                    throws TimeFrameFactoryInitializationException {
        this.access = access;
    }

    /**
     * Auxiliary method for extracting fine-grained read or write data.
     *
     * @param clzz
     * @param timeFrame
     * @param field
     * @param value
     * @return
     * @throws BillingQueryException
     */
    protected <T extends IPlotData> Collection<IPlotData> getFineGrainedPlotData(
                    Class<T> clzz, TimeFrame timeFrame, String field,
                    String type, Object value) throws BillingQueryException {
        return getPlotData(
                        clzz,
                        field
                        + " == value && dateStamp >= date1 && dateStamp < date2 && errorCode == 0",
                        type
                        + " value, java.util.Date date1, java.util.Date date2",
                        value, timeFrame.getLow(), timeFrame.getHigh());
    }

    /**
     * Auxiliary method for extracting fine-grained data.
     *
     * @param clzz
     * @param timeFrame
     * @return
     * @throws BillingQueryException
     */
    protected <T extends IPlotData> Collection<IPlotData> getFineGrainedPlotData(
                    Class<T> clzz, TimeFrame timeFrame)
                                    throws BillingQueryException {
        return getPlotData(clzz, "dateStamp >= date1 && dateStamp < date2 && errorCode == 0",
                        "java.util.Date date1, java.util.Date date2",
                        timeFrame.getLow(), timeFrame.getHigh());
    }

    /**
     * Auxiliary method for extracting coarse-grained data.
     *
     * @param clzz
     * @param timeFrame
     * @return
     * @throws BillingQueryException
     */
    protected <T extends IPlotData> Collection<IPlotData> getCoarseGrainedPlotData(
                    Class<T> clzz, TimeFrame timeFrame)
                                    throws BillingQueryException {
        return getPlotData(clzz, "date >= date1 && date <= date2",
                        "java.util.Date date1, java.util.Date date2",
                        timeFrame.getLow(), timeFrame.getHigh());
    }

    /**
     * All-purpose extraction auxiliary.
     *
     * @param clzz
     * @param filter
     * @param values
     * @return
     * @throws BillingQueryException
     */
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
