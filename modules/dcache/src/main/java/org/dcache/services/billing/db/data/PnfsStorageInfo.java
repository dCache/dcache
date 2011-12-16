package org.dcache.services.billing.db.data;

import java.util.Map;

import diskCacheV111.vehicles.PnfsFileInfoMessage;

/**
 * Superclass of all billing storage (HSM) actions.
 *
 * @author arossi
 */
public abstract class PnfsStorageInfo extends PnfsConnectInfo {

    public static final String FULL_SIZE = "fullSize";

    protected Long fullSize = 0L;
    protected String storageClass;

    /**
     * Required by Datanucleus.
     */
    protected PnfsStorageInfo() {
    }

    /**
     * @param info dcache-internal object used for messages
     * @param connectionTime
     * @param size
     */
    protected PnfsStorageInfo(PnfsFileInfoMessage info, Long connectionTime, Long size) {
        super(info, connectionTime);
        fullSize = size;
    }

    /**
     * @return the fullSize
     */
    public Long getFullSize() {
        return fullSize;
    }

    /**
     * @param fullSize
     *            the fullSize to set
     */
    public void setFullSize(Long fullSize) {
        this.fullSize = fullSize;
    }

    /**
     * @return the storageClass
     */
    public String getStorageClass() {
        return storageClass;
    }

    /**
     * @param storageClass
     *            the storageClass to set
     */
    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.data.ITimestampedData#data()
     */
    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(FULL_SIZE, fullSize.doubleValue());
        return dataMap;
    }
}
