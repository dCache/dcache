package org.dcache.acl.util.net;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Matcher for IPv4/IPv6 network addresses.
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class InetAddressMatcher {

    public static boolean matches(String netmask, InetAddress inetAddress)
    {
        if ( netmask == null || netmask.length() == 0 ) {
            throw new IllegalArgumentException("netmask is " + (netmask == null ? "NULL" : "Empty"));
        }

        if ( inetAddress == null ) {
            throw new IllegalArgumentException("inetAddress is NULL.");
        }

        if ( inetAddress instanceof Inet4Address ) {
            return Inet4AddressMatcher
                    .matches(netmask, (Inet4Address) inetAddress);
        } else if ( inetAddress instanceof Inet6Address ) {
            return Inet6AddressMatcher
                    .matches(netmask, (Inet6Address) inetAddress);
        } else {
            throw new IllegalArgumentException("Unsupported type of inetAddress: " + inetAddress);
        }
    }

    public static boolean matches(String netmask, String host) throws UnknownHostException {
        return matches(netmask, InetAddress.getByName(host));
    }
}
