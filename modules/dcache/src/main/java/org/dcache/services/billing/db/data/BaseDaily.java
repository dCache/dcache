package org.dcache.services.billing.db.data;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for all objects carrying daily aggregated data.
 *
 * @author arossi
 */
public abstract class BaseDaily implements IPlotData {
    public static final String COUNT = "count";

    protected Date date  = new Date(System.currentTimeMillis());
    protected Long count = 0L;

    /**
     * @return the date
     */
    public Date getDate() {
        return date;
    }

    /**
     * @param date
     *            the date to set
     */
    public void setDate(Date date) {
        this.date = date;
    }

    /**
     * @return the count
     */
    public Long getCount() {
        return count;
    }

    /**
     * @param count
     *            the count to set
     */
    public void setCount(Long count) {
        this.count = count;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.data.ITimestamped#timestamp()
     */
    @Override
    public Date timestamp() {
        return date;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.services.billing.db.data.IPlotData#data()
     */
    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = new ConcurrentHashMap<String, Double>();
        dataMap.put(COUNT, count.doubleValue());
        return dataMap;
    }

    /**
     * @return string using the predefined format.
     */
    protected String dateString() {
        DateFormat formatter = new SimpleDateFormat(
                        PnfsBaseInfo.DATE_FORMAT);
        return formatter.format(date);
    }
}
