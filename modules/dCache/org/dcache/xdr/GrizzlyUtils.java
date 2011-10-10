package org.dcache.xdr;

import org.glassfish.grizzly.Transport;
import org.glassfish.grizzly.filterchain.Filter;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.UDPNIOTransport;

/**
 * Class with utility methods for Grizzly
 */
public class GrizzlyUtils {
    private GrizzlyUtils(){}

    public static Filter rpcMessageReceiverFor(Transport t) {
        if (t instanceof TCPNIOTransport) {
            return new RpcMessageParserTCP();
        }

        if (t instanceof UDPNIOTransport) {
            return new RpcMessageParserUDP();
        }

        throw new RuntimeException("Unsupported transport: " + t.getClass().getName());
    }

    public static Class< ? extends Transport> transportFor(int protocol) {
        switch(protocol) {
            case IpProtocolType.TCP:
                return TCPNIOTransport.class;
            case IpProtocolType.UDP:
                return UDPNIOTransport.class;
        }
        throw new RuntimeException("Unsupported protocol: " + protocol);
    }
}
