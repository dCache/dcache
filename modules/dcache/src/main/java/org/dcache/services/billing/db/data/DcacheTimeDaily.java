package org.dcache.services.billing.db.data;

import java.util.Map;

/**
 * @author arossi
 *
 */
public final class DcacheTimeDaily extends BaseDaily implements IPlotData {

    public static final String MIN_TIME = "minimum";
    public static final String MAX_TIME = "maximum";
    public static final String AVG_TIME = "average";

    private Long totalTime = Long.MAX_VALUE;
    private Long minimum= 0L;
    private Long maximum= 0L;

    public String toString() {
        return "(" + dateString() + "," + count + "," + minimum + "," + maximum
                        + "," + average() + ")";
    }

    /**
     * @return the totalTime
     */
    public Long getTotalTime() {
        return totalTime;
    }

    /**
     * @param totalTime
     *            the totalTime to set
     */
    public void setTotalTime(Long totalTime) {
        this.totalTime = totalTime;
    }

    /**
     * @return the minimum
     */
    public Long getMinimum() {
        return minimum;
    }

    /**
     * @param minimum
     *            the minimum to set
     */
    public void setMinimum(Long minimum) {
        this.minimum = minimum;
    }

    /**
     * @return the maximum
     */
    public Long getMaximum() {
        return maximum;
    }

    /**
     * @param maximum
     *            the maximum to set
     */
    public void setMaximum(Long maximum) {
        this.maximum = maximum;
    }

    /**
     * @return the average
     */
    public Double average() {
        if (count == 0)
            return 0.0;
        return totalTime / (double) count;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.data.ITimestampedData#data()
     */
    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(MIN_TIME, minimum.doubleValue());
        dataMap.put(MAX_TIME, maximum.doubleValue());
        dataMap.put(AVG_TIME, average().doubleValue());
        return dataMap;
    }
}
