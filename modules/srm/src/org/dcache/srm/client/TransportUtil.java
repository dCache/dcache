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
            return "httpg";
        }
        /* We return httpg for Transport.SSL since we're using JGlobus libraries.
         * When using standard libraries for SSL, this should be changed to
         * "https" once we switch away from JGlobus.
         */
    }

    public static boolean hasGsiMode(Transport transport) {
        return transport == Transport.SSL || transport == Transport.GSI;
    }

    public static String gsiModeFor(Transport transport, boolean do_delegation, boolean full_delegation) {
        switch( transport) {
        case GSI:
            if (do_delegation) {
                return  full_delegation ?
                        org.globus.axis.transport.GSIHTTPTransport.GSI_MODE_FULL_DELEG :
                            org.globus.axis.transport.GSIHTTPTransport.GSI_MODE_LIMITED_DELEG;
            } else {
                return org.globus.axis.transport.GSIHTTPTransport.GSI_MODE_NO_DELEG;
            }

        case SSL:
            return org.globus.axis.transport.GSIHTTPTransport.GSI_MODE_SSL;

        case TCP:
            throw new RuntimeException("No GSI mode needed for TCP transport");
        }

        throw new RuntimeException("Unknown transport: " + transport);
    }
}
