package org.dcache.services.billing.plots.util;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Data object which aids in the construction of the X-axis of a 1-D histogram.<br>
 * <br>
 * Contains convenience methods for computing and returning the number of bins
 * and the bin width based on the upper bound time value, {@link BinType} and
 * the {@link Type}.
 *
 * @author arossi
 */
public class TimeFrame {

    /**
     * @param hourly
     *            bin; if false, daily bin is assumed
     * @return Current time rounded up to next hour for hourly and
     *         to the next day for daily data.
     */
    public static Calendar computeHighTimeFromNow(BinType type) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        int hh = 0;
        int dd = cal.get(Calendar.DAY_OF_MONTH);
        if (type == BinType.HOUR) {
            hh = cal.get(Calendar.HOUR_OF_DAY) + 1;
        } else {
            dd++;
        }
        cal.set(Calendar.DAY_OF_MONTH, dd);
        cal.set(Calendar.HOUR_OF_DAY, hh);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }

    /**
     * Extent of the time frame.
     */
    public enum Type {
        DAY, WEEK, MONTH, YEAR, THIS_DAY, THIS_WEEK, THIS_MONTH, THIS_YEAR
    }

    /**
     * Unit of the time frame.
     */
    public enum BinType {
        TEN_MINUTE, HOUR, DAY, WEEK, MONTH
    }

    private Date low;
    private Date high;
    private int binCount;
    private double binWidth;
    private Type timeframe;
    private BinType timebin;
    private Calendar highDate, lowDate;

    /**
     * Defaults to current time for upper bound.
     */
    public TimeFrame() {
        this(System.currentTimeMillis());
    }

    /**
     * @param highTime
     *            upper bound
     */
    public TimeFrame(long highTime) {
        high = new Date(highTime);
        highDate = Calendar.getInstance();
        lowDate = Calendar.getInstance();
        binCount = 1;
        timeframe = Type.DAY;
    }

    /**
     * computes boundaries, number of bins and bin width.
     */
    public void configure() {
        Calendar cdate = Calendar.getInstance();
        cdate.setTime(high);

        if (timeframe == Type.DAY) {
            low = new Date(high.getTime() - TimeUnit.DAYS.toMillis(1));
        } else if (timeframe == Type.WEEK) {
            low = new Date(high.getTime() - TimeUnit.DAYS.toMillis(7));
        } else if (timeframe == Type.MONTH) {
            low = new Date(high.getTime() - TimeUnit.DAYS.toMillis(30));
        } else if (timeframe == Type.YEAR) {
            low = new Date(high.getTime() - TimeUnit.DAYS.toMillis(365));
        } else if (timeframe == Type.THIS_DAY) {
            cdate.set(Calendar.MINUTE, 0);
            cdate.set(Calendar.SECOND, 0);
            cdate.set(Calendar.HOUR_OF_DAY, 0);
            low = new Date(cdate.getTimeInMillis());
            cdate.set(Calendar.MINUTE, 59);
            cdate.set(Calendar.SECOND, 59);
            cdate.set(Calendar.HOUR_OF_DAY, 23);
            high = new Date(cdate.getTimeInMillis());
        } else if (timeframe == Type.THIS_WEEK) {
            cdate.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
            cdate.set(Calendar.SECOND, 0);
            cdate.set(Calendar.MINUTE, 0);
            cdate.set(Calendar.HOUR_OF_DAY, 0);
            low = new Date(cdate.getTimeInMillis());
            cdate.set(Calendar.SECOND, 59);
            cdate.set(Calendar.MINUTE, 59);
            cdate.set(Calendar.HOUR_OF_DAY, 23);
            cdate.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
            high = new Date(cdate.getTimeInMillis());
        } else if (timeframe == Type.THIS_MONTH) {
            cdate.set(Calendar.DAY_OF_MONTH, 1);
            cdate.set(Calendar.SECOND, 0);
            cdate.set(Calendar.MINUTE, 0);
            cdate.set(Calendar.HOUR_OF_DAY, 0);
            low = new Date(cdate.getTimeInMillis());
            cdate.set(Calendar.SECOND, 59);
            cdate.set(Calendar.MINUTE, 59);
            cdate.set(Calendar.HOUR_OF_DAY, 23);
            cdate.set(Calendar.DAY_OF_MONTH,
                            cdate.getActualMaximum(Calendar.DAY_OF_MONTH));
            high = new Date(cdate.getTimeInMillis());
        } else if (timeframe == Type.THIS_YEAR) {
            cdate.set(Calendar.DAY_OF_MONTH, 1);
            cdate.set(Calendar.MONTH, Calendar.JANUARY);
            cdate.set(Calendar.SECOND, 0);
            cdate.set(Calendar.MINUTE, 0);
            cdate.set(Calendar.HOUR_OF_DAY, 0);
            low = new Date(cdate.getTimeInMillis());
            cdate.set(Calendar.MONTH, Calendar.DECEMBER);
            cdate.set(Calendar.SECOND, 59);
            cdate.set(Calendar.MINUTE, 59);
            cdate.set(Calendar.HOUR_OF_DAY, 23);
            cdate.set(Calendar.DAY_OF_MONTH,
                            cdate.getActualMaximum(Calendar.DAY_OF_MONTH));
            high = new Date(cdate.getTimeInMillis());
        }

        if (timebin == BinType.TEN_MINUTE) {
            binWidth = 10 * 60;
        } else if (timebin == BinType.HOUR) {
            binWidth = 3600;
        } else if (timebin == BinType.DAY) {
            binWidth = 3600 * 24;
        } else if (timebin == BinType.WEEK) {
            binWidth = 3600 * 24 * 7;
        } else if (timebin == BinType.MONTH) {
            binWidth = 3600 * 24 * 30;
        } else {
            binWidth = 3600;
        }

        binCount = (int) ((getHighTime().doubleValue() / 1000 - getLowTime()
                        .doubleValue() / 1000) / binWidth);
    }

    /**
     * @return milliseconds
     */
    public Long getLowTime() {
        return low.getTime();
    }

    /**
     * @return milliseconds
     */
    public Long getHighTime() {
        return high.getTime();
    }

    public Date getLow() {
        return new Date(low.getTime());
    }

    public Date getHigh() {
        return new Date(high.getTime());
    }

    public int getBinCount() {
        return binCount;
    }

    public double getBinWidth() {
        return binWidth;
    }

    public Calendar getHighDate() {
        return highDate;
    }

    public Calendar getLowDate() {
        return lowDate;
    }

    public Type getTimeframe() {
        return timeframe;
    }

    public void setTimeframe(Type timeframe) {
        this.timeframe = timeframe;
    }

    public BinType getTimebin() {
        return timebin;
    }

    public void setTimebin(BinType timebin) {
        this.timebin = timebin;
    }
}
