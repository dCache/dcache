package diskCacheV111.vehicles;

import java.net.InetSocketAddress;

public interface IpProtocolInfo extends ProtocolInfo {

    @Deprecated
    public String[] getHosts();

    @Deprecated
    public int getPort();

    /**
     * Returns clients {@link InetSocketAddress}
     */
    InetSocketAddress getSocketAddress();
}
