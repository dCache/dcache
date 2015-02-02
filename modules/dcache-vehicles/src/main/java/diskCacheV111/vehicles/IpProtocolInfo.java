package diskCacheV111.vehicles;

import java.net.InetSocketAddress;

public interface IpProtocolInfo extends ProtocolInfo {

    /**
     * Returns clients {@link InetSocketAddress}
     */
    InetSocketAddress getSocketAddress();
}
