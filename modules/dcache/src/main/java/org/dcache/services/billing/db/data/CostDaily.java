package org.dcache.services.billing.db.data;

import java.util.Map;

/**
 * @author arossi
 */
public final class CostDaily extends BaseDaily {
    public static final String TOTAL_COST = "totalCost";

    private Double totalCost = 0.0;

    public String toString() {
        return "(" + dateString() + "," + count + "," + totalCost + ")";
    }

    /**
     * @return the totalCost
     */
    public Double getTotalCost() {
        return totalCost;
    }

    /**
     * @param totalCost
     *            the totalCost to set
     */
    public void setTotalCost(Double totalCost) {
        this.totalCost = totalCost;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.data.ITimestampedData#data()
     */
    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(TOTAL_COST, totalCost);
        return dataMap;
    }
}
