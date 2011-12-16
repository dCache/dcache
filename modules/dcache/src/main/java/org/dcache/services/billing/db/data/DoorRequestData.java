package org.dcache.services.billing.db.data;

import java.util.Map;

import diskCacheV111.vehicles.DoorRequestInfoMessage;

/**
 * @author arossi
 */
public final class DoorRequestData extends PnfsConnectInfo {

    public static final String QUEUED_TIME = "queuedTime";

    public String toString() {
        return "(" + dateString() + "," + cellName + "," + action + ","
                        + owner + "," + mappedUID + "," + mappedGID + ","
                        + "," + client + "," + transaction + "," + pnfsID
                        + "," + connectionTime + "," + queuedTime + ","
                        + errorCode + "," + errorMessage + "," + path + ")";
    }

    private String owner;
    private Integer mappedUID;
    private Integer mappedGID;
    private String client;
    private String path;
    private Long queuedTime;

    /**
     * Required by Datanucleus.
     */
    public DoorRequestData() {
        queuedTime = 0L;
    }

    /**
     * @param info
     *            dcache-internal object used for messages
     */
    public DoorRequestData(DoorRequestInfoMessage info) {
        super(info, info.getTransactionDuration());
        queuedTime = info.getTimeQueued();
        owner = info.getOwner();
        mappedUID = info.getUid();
        mappedGID = info.getGid();
        client = info.getClient();
        path = info.getPath();
    }

    /**
     * @return the owner
     */
    public String getOwner() {
        return owner;
    }

    /**
     * @param owner
     *            the owner to set
     */
    public void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     * @return the mappedUID
     */
    public Integer getMappedUID() {
        return mappedUID;
    }

    /**
     * @param mappedUID
     *            the mappedUID to set
     */
    public void setMappedUID(Integer mappedUID) {
        this.mappedUID = mappedUID;
    }

    /**
     * @return the _mappedGID
     */
    public Integer getMappedGID() {
        return mappedGID;
    }

    /**
     * @param mappedGID
     *            the mappedGID to set
     */
    public void setMappedGID(Integer mappedGID) {
        this.mappedGID = mappedGID;
    }

    /**
     * @return the client
     */
    public String getClient() {
        return client;
    }

    /**
     * @param client
     *            the client to set
     */
    public void setClient(String client) {
        this.client = client;
    }

    /**
     * @return the _queuedTime
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

    /**
     * @return the path
     */
    public String getPath() {
        return path;
    }

    /**
     * @param path
     *            the path to set
     */
    public void setPath(String path) {
        this.path = path;
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
