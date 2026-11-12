package diskCacheV111.vehicles;

import java.net.InetSocketAddress;

public interface IpProtocolInfo extends ProtocolInfo {

    /**
     * Returns clients {@link InetSocketAddress}
     */
    InetSocketAddress getSocketAddress();

    /**
     * Returns the address of the remote party whose data this transfer carries, for accounting.
     * This differs from {@link #getSocketAddress} when the data is relayed, as the socket address
     * is then that of the relaying door.
     */
    default InetSocketAddress getOriginAddress() {
        return getSocketAddress();
    }
}
