package org.dcache.services.billing.db.data;

import java.util.Map;

import diskCacheV111.vehicles.PoolCostInfoMessage;

/**
 * @author arossi
 */
public final class PoolCostData extends PnfsBaseInfo {

    public static final String COST = "cost";

    public String toString() {
        return "(" + dateString() + "," + cellName + "," + action + ","
                        + transaction + "," + pnfsID + "," + cost + ","
                        + errorCode + "," + errorMessage + ")";
    }

    private Double cost;

    /**
     * Required by Datanucleus.
     */
    public PoolCostData() {
        cost = 0.0;
    }

    /**
     * @param info dcache-internal object used for messages
     */
    public PoolCostData(PoolCostInfoMessage info) {
        super(info);
        cost = info.getCost();
    }

    /**
     * @return the cost
     */
    public Double getCost() {
        return cost;
    }

    /**
     * @param cost
     *            the cost to set
     */
    public void setCost(Double cost) {
        this.cost = cost;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.data.ITimestampedData#data()
     */
    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(COST, cost);
        return dataMap;
    }
}
