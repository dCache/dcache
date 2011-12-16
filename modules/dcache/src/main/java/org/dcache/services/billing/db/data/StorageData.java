package org.dcache.services.billing.db.data;

import java.util.Map;

import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfoMessage;

/**
 * @author arossi
 */
public final class StorageData extends PnfsStorageInfo {

    public static final String QUEUED_TIME = "queuedTime";

    public String toString() {
        return "(" + dateString() + "," + cellName + "," + action + ","
                        + transaction + "," + pnfsID + "," + fullSize + ","
                        + storageClass + "," + connectionTime + ","
                        + queuedTime + "," + errorCode + "," + errorMessage
                        + ")";
    }

    private Long queuedTime;

    /**
     * Required by Datanucleus.
     */
    public StorageData() {
        queuedTime = 0L;
    }

    /**
     * @param info dcache-internal object used for messages
     */
    public StorageData(StorageInfoMessage info) {
        super(info, info.getTransferTime(), info.getFileSize());
        StorageInfo sinfo = info.getStorageInfo();
        if (sinfo != null) {
            storageClass = sinfo.getStorageClass()
                            + "@"
                            + sinfo.getHsm();
        }
        queuedTime = info.getTimeQueued();
    }

    /**
     * @return the queuedTime
     */
    public Long getQueuedTime() {
        return queuedTime;
    }

    /**
     * @param queuedTime
     *            the queuedTime to set
     */
    public void setQueuedTime(Long queuedTime) {
        this.queuedTime = queuedTime;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.data.ITimestampedData#data()
     */
    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(QUEUED_TIME, queuedTime.doubleValue());
        return dataMap;
    }
}
