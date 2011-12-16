package org.dcache.services.billing.db.data;

import diskCacheV111.vehicles.PoolHitInfoMessage;

/**
 * @author arossi
 */
public final class PoolHitData extends PnfsBaseInfo {

    public String toString() {
        return "(" + dateString() + "," + cellName + "," + action + ","
                        + transaction + "," + pnfsID + "," + fileCached
                        + "," + errorCode + "," + errorMessage + ")";
    }

    private Boolean fileCached;

    /**
     * Required by Datanucleus.
     */
    public PoolHitData() {
        fileCached = false;
    }

    /**
     * @param info dcache-internal object used for messages
     */
    public PoolHitData(PoolHitInfoMessage info) {
        super(info);
        fileCached = info.getFileCached();
    }

    /**
     * @return the fileCached
     */
    public Boolean getFileCached() {
        return fileCached;
    }

    /**
     * @param fileCached
     *            the fileCached to set
     */
    public void setFileCached(Boolean fileCached) {
        this.fileCached = fileCached;
    }
}
