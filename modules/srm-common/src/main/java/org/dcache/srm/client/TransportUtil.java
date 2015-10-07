package org.dcache.srm.client;

/**
 * Utility methods for working with Transport
 */
public final class TransportUtil {

    private TransportUtil() {
        // Prevent instantiation.
    }

    public static String uriSchemaFor( Transport transport) {
        if(transport == Transport.TCP) {
            return "http";
        } else {
            return "https";
        }
    }

    public static HttpClientTransport.Delegation delegationModeFor(Transport transport, boolean do_delegation, boolean full_delegation)
    {
        switch (transport) {
        case TCP:
            return HttpClientTransport.Delegation.SKIP;
        case GSI:
            if (do_delegation) {
                return full_delegation ? HttpClientTransport.Delegation.FULL : HttpClientTransport.Delegation.LIMITED;
            } else {
                return HttpClientTransport.Delegation.NONE;
            }
        case SSL:
            return HttpClientTransport.Delegation.SKIP;
        default:
            throw new IllegalArgumentException();
        }
    }
}
