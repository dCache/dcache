package org.dcache.util;

import java.net.InetAddress;

/**
 *
 * @author Daniel Becker
 */
class NetworkReality {
    public String getHostNameFor(InetAddress ip) {
        return ip.getHostName();
    }
}
