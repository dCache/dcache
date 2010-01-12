package diskCacheV111.vehicles;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import org.dcache.util.NetworkUtils;

public abstract class IpProtocolInfo implements ProtocolInfo {

    private static final int FIRST_CLIENT_HOST = 0;

    public abstract String[] getHosts();

    public abstract int getPort();

    public InetAddress getLocalAddressForClient() throws SocketException, UnknownHostException {
        // try to pick the ip address with corresponds to the
        // hostname (which is hopefully visible to the world)
        // by service method
        InetAddress clientAddress = InetAddress.getByName(this.getHosts()[FIRST_CLIENT_HOST]);
        InetAddress localAddress = NetworkUtils.getLocalAddress(clientAddress);
        return localAddress;
    }
}
