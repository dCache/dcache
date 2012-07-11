package org.dcache.acl.util.net;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Matcher for IPv4 network addresses. The InetAddressMatcher can match the
 * following kinds of IP addresses or address ranges:
 * <ul>
 * <li>IP address only (<code>192.168.1.2</code>)</li>
 * <li>IP address and network mask (<code>192.168.1.2/255.255.255.0</code>)</li>
 * <li>IP address and network mask bits (<code>192.168.1.2/24</code>)</li>
 * </ul>
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class Inet4AddressMatcher {

    private static final char ADDRESS_SEPARATOR = '/';

    private static final char MASK_SEPARATOR = '.';

    /**
     * The address of this matcher
     */
    private int _address;

    /**
     * The network mask of this matcher
     */
    private int _netmask;

    /**
     * Creates a new address matcher.
     *
     * @param address
     *            of this matcher
     * @param netmask
     *            of this matcher
     */
    public Inet4AddressMatcher(String netmask, InetAddress address) throws UnknownHostException {
        super();
        _netmask = (new BigInteger(netmask, 16)).intValue();
        _address = address2int(address);
    }

    /**
     * Creates a new address matcher.
     *
     * @param hostname
     *            The address range this matcher matches
     */
    public Inet4AddressMatcher(String hostname) throws UnknownHostException {
        int pos = hostname.indexOf(ADDRESS_SEPARATOR);
        if ( pos == -1 ) {
            _address = address2int(hostname);
            _netmask = 0xFFFFFFFF;

        } else {
            _address = address2int(hostname.substring(0, pos));
            String maskPart = hostname.substring(pos + 1);
            if ( maskPart.indexOf(MASK_SEPARATOR) == -1 ) {
                _netmask = 0xFFFFFFFF << (32 - Integer.parseInt(maskPart));
                if ( Integer.parseInt(maskPart) == 0 ) {
                    _netmask = 0;
                }

            } else {
                _netmask = address2int(maskPart);
            }
        }
    }

    /**
     * Converts a dotted IP address (a.b.c.d) to a 32-bit value.
     *
     * @param address
     *            The address to convert
     * @return The IP address as 32-bit value
     * @throws UnknownHostException
     */
    public static int address2int(InetAddress address) {
        return (new BigInteger(address.getAddress()).intValue());
    }
    public static int address2int(String address)  throws UnknownHostException {
        return (new BigInteger(InetAddress.getByName(address).getAddress()).intValue());
    }

    /**
     * Checks whether the given address matches this matcher's address.
     *
     * @param address
     *            The address to match to this matcher
     * @return <code>true</code> if <code>hostname</code> matches the
     *         specification of this matcher, <code>false</code> otherwise
     */
    public boolean matches(InetAddress address) {
        int matchAddress = address2int(address);
        return (matchAddress /* & netmask */) == (_address & _netmask);
    }

    /**
     * @param netmask
     *            The network mask to match
     * @param inetAddress
     *            The address to match
     * @return <code>true</code> if <code>address</code> matches the
     *         <code>netmask</code>, otherwise <code>false</code>
     *
     */
    public static boolean matches(String netmask, InetAddress inetAddress) throws UnknownHostException {
        Inet4AddressMatcher addressmatcher = new Inet4AddressMatcher(netmask, inetAddress);
        return addressmatcher.matches(inetAddress);
    }
    public static boolean matches(String netmask, String address) throws UnknownHostException {
        return matches(netmask, Inet4Address.getByName(address));
    }
}
