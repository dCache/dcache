package org.dcache.services.billing.db.data;

import java.util.Map;

import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.StorageInfo;

/**
 * @author arossi
 */
public final class MoverData extends PnfsStorageInfo {

    public static final String TRANSFER_SIZE = "transferSize";

    private static final String DEFAULT_PROTOCOL = "<unknown>";

    public String toString() {
        return "(" + dateString() + "," + cellName + "," + action + ","
                        + transaction + "," + pnfsID + "," + fullSize + ","
                        + transferSize + "," + storageClass + "," + isNew
                        + "," + client + "," + connectionTime + ","
                        + errorCode + "," + errorMessage + "," + protocol
                        + "," + initiator + ")";
    }

    private Long transferSize;
    private Boolean isNew;
    private String client;
    private String protocol;
    private String initiator;

    /**
     * Needed by Datanucleus.
     */
    public MoverData() {
        transferSize = 0L;
        isNew = false;
        protocol = DEFAULT_PROTOCOL;
        client = DEFAULT_PROTOCOL;
    }

    /**
     * @param info
     *            dcache-internal object used for messages
     */
    @SuppressWarnings("deprecation")
    public MoverData(MoverInfoMessage info) {
        super(info, info.getConnectionTime(), info.getFileSize());
        transferSize = info.getDataTransferred();
        isNew = info.isFileCreated();

        if (info.getProtocolInfo() instanceof IpProtocolInfo) {
            String[] clients = ((IpProtocolInfo) info.getProtocolInfo()).getHosts();
            protocol = ((IpProtocolInfo) info.getProtocolInfo())
                            .getVersionString();
            client = clients[0];
        } else {
            protocol = DEFAULT_PROTOCOL;
            client = DEFAULT_PROTOCOL;
        }

        StorageInfo sinfo = info.getStorageInfo();
        if (sinfo != null) {
            storageClass = sinfo.getStorageClass()
                            + "@"
                            + sinfo.getHsm();
        }

        initiator = info.getInitiator();
    }

    /**
     * @return the transferSize
     */
    public Long getTransferSize() {
        return transferSize;
    }

    /**
     * @param transferSize
     *            the transferSize to set
     */
    public void setTransferSize(Long transferSize) {
        this.transferSize = transferSize;
    }

    /**
     * @return the isNew
     */
    public Boolean getIsNew() {
        return isNew;
    }

    /**
     * @param isNew
     *            the isNew to set
     */
    public void setIsNew(Boolean isNew) {
        this.isNew = isNew;
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
     * @return the protocol
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * @param protocol
     *            the protocol to set
     */
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    /**
     * @return the initiator
     */
    public String getInitiator() {
        return initiator;
    }

    /**
     * @param initiator
     *            the initiator to set
     */
    public void setInitiator(String initiator) {
        this.initiator = initiator;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.data.ITimestampedData#data()
     */
    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(TRANSFER_SIZE, transferSize.doubleValue());
        return dataMap;
    }
}
