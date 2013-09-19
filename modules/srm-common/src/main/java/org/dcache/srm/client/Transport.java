package org.dcache.srm.client;

/**
 * A class describing which transport the client will use when connecting to
 * a remote storage element.
 */
public enum Transport {
    /** An unencrypted transport, using TCP */
    TCP,

    /** An encrypted transport, using GSI protocol (over TCP) */
    GSI,

    /** Encrypted transport, using SSL protocol (over TCP) */
    SSL;

    static private final String COMMA_SEPARATED_LIST;

    static {
        StringBuilder sb = new StringBuilder();
        Transport[] transports = Transport.values();
        Transport lastTransport = transports[transports.length - 1];

        for( Transport transport : transports) {
            sb.append( transport.name());
            if( transport != lastTransport) {
                sb.append( ", ");
            }
        }

        COMMA_SEPARATED_LIST = sb.toString();
    }

    /**
     * Provide the Transport that matches the given name. The name should be
     * provided by the {@link #getName} method.
     */
    static public Transport transportFor( String name) {
        for( Transport t : Transport.values()) {
            if( t.name().equalsIgnoreCase( name)) {
                return t;
            }
        }

        throw new IllegalArgumentException( "Unknown Transport " + name +
                                            ", value not from {" +
                                            COMMA_SEPARATED_LIST + "}");
    }
}
