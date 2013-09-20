package org.dcache.services.billing.db.data;

import java.util.Map;

/**
 * @author arossi
 */
public final class DcacheTimeDaily extends BaseDaily implements IPlotData {
    public static final String MIN_TIME = "minimum";
    public static final String MAX_TIME = "maximum";
    public static final String AVG_TIME = "average";

    private Long minimum= 0L;
    private Long maximum= 0L;
    private Double average = 0.0;

    public String toString() {
        return "(" + dateString() + "," + count + "," + minimum + "," + maximum
                        + "," + average + ")";
    }

    public Double getAverage() {
        return average;
    }

    public void setAverage(Double average) {
        this.average = average;
    }

    public Long getMinimum() {
        return minimum;
    }

    public void setMinimum(Long minimum) {
        this.minimum = minimum;
    }

    public Long getMaximum() {
        return maximum;
    }

    public void setMaximum(Long maximum) {
        this.maximum = maximum;
    }

    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(MIN_TIME, minimum.doubleValue());
        dataMap.put(MAX_TIME, maximum.doubleValue());
        dataMap.put(AVG_TIME, average);
        return dataMap;
    }
}
