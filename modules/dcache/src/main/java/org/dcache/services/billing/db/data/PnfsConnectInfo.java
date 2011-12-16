package org.dcache.services.billing.db.data;

import java.util.Map;

import diskCacheV111.vehicles.PnfsFileInfoMessage;

/**
 * Superclass of all billing actions which are timestamped.
 *
 * @author arossi
 */
public abstract class PnfsConnectInfo extends PnfsBaseInfo {

    public static final String CONNECTION_TIME = "connectionTime";

    protected Long connectionTime = 0L;

    /**
     * Required by Datanucleus.
     */
    protected PnfsConnectInfo() {
    }

    /**
     * @param info dcache-internal object used for messages
     * @param connectionTime
     */
    protected PnfsConnectInfo(PnfsFileInfoMessage info, Long connectionTime) {
        super(info);
        this.connectionTime = connectionTime;
    }

    /**
     * @return the connectionTime
     */
    public Long getConnectionTime() {
        return connectionTime;
    }

    /**
     * @param connectionTime
     *            the connectionTime to set
     */
    public void setConnectionTime(Long connectionTime) {
        this.connectionTime = connectionTime;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.data.ITimestampedData#data()
     */
    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(CONNECTION_TIME, connectionTime.doubleValue());
        return dataMap;
    }

}
