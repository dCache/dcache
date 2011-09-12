package org.dcache.util;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.InetAddresses.forString;
import static com.google.common.net.InetAddresses.toUriString;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Subnet {

    private static final Pattern IPV4_PATTERN = Pattern.compile("((?:\\d{1,3}\\.){3}\\d{1,3})(?:/(\\d{1,2}))?");
    private static final Pattern IPV6_PATTERN = Pattern.compile("((?:::)|(?:(?:(?:[0-9a-fA-F]{1,4}:)|:){1,7})[0-9a-fA-F]{1,4})(?:/(\\d{1,3}))?");

    private static final int HOST_IP_GROUP_INDEX   = 1;
    private static final int MASK_BITS_GROUP_INDEX = 2;

    private InetAddress _subnetAddress;
    private int _mask;

    private Subnet() {}

    private Subnet(InetAddress subnetAddress, int mask) {
        _subnetAddress = subnetAddress;
        _mask = mask;
    }

    public InetAddress getSubnetAddress() {
        return _subnetAddress;
    }

    public int getMask() {
        return _mask;
    }

    /**
     * Creates an instance of a subnet from an InetAddress and a mask in CIDR notation.
     *
     * @param subnetAddress
     * @param mask mask in CIDR notation (number of topmost relevant bits)
     * @return
     */
    public static Subnet create(InetAddress subnetAddress, int mask) {
        return new Subnet(subnetAddress, mask);
    }

    /**
     * Creates an instance of a subnet from it's CIDR notation.
     *
     * Examples: "123.45.0.0/24"; "1234:5678:9ABC:DEF0::/64"
     *
     * @param cidrPattern CIDR notation of the subnet
     * @return
     */
    public static Subnet create(String cidrPattern) {
        Matcher matcher;
        if ((matcher = IPV4_PATTERN.matcher(cidrPattern)).matches()) {
            String netmaskGroup = matcher.group(MASK_BITS_GROUP_INDEX);
            return create(forString(matcher.group(HOST_IP_GROUP_INDEX)), isNullOrEmpty(netmaskGroup) ? 32 : Integer.parseInt(netmaskGroup));
        } else if ((matcher = IPV6_PATTERN.matcher(cidrPattern)).matches()) {
            String netmaskGroup = matcher.group(MASK_BITS_GROUP_INDEX);
            return create(forString(matcher.group(HOST_IP_GROUP_INDEX)), isNullOrEmpty(netmaskGroup) ? 128 : Integer.parseInt(netmaskGroup));
        }
        throw new IllegalArgumentException("\"" + cidrPattern + "\" is not a valid CIDR pattern.");
    }

    /**
     * @return an instance of Subnet that matches all IPs
     */
    public static Subnet create() {
        return new Subnet() {
            @Override
            public boolean contains(InetAddress inetAddress) {
                return true;
            }
        };
    }

    /**
     * creates the string representation of an IPv4 InetAddress
     * @param inetAddress
     * @return a string representation of the InetAddress (e.g., "192.168.0.3")
     */
    public static String toIPv4String(Inet4Address inetAddress) {
        return toUriString(inetAddress);
    }

    /**
     * creates the string representation of an IPv6 InetAddress
     * @param inetAddress
     * @return a string representation of the InetAddress (e.g., "::1")
     */
    public static String toIPv6String(Inet6Address inetAddress) {
        byte[] address = inetAddress.getAddress();

        StringBuilder sb = new StringBuilder(Integer.toHexString(address[0] << 8 | (address[1] & 0xff)));
        for (int i = 2; i<16; i+=2) {
            sb.append(":" + Integer.toHexString(address[i] << 8 | (address[i+1] & 0xff) ));
        }
        return sb.toString().replaceFirst("((:|^)0){2,}:", "::");
    }


    /**
     * @param hostname
     * @return true if hostname can be evaluated to one or more IPs that are contained in the subnet represented by this instance, otherwise false
     * @throws UnknownHostException
     */
    public boolean containsHost(String hostname) throws UnknownHostException {
        return containsAny(InetAddress.getAllByName(hostname));
    }

    /**
     * @param inetAddresses
     * @return true if any of the inetAddresses is contained in the subnet represented by this instance, otherwise false
     */
    public boolean containsAny(InetAddress[] inetAddresses) {
        for (InetAddress inetAddress : inetAddresses) {
            if (contains(inetAddress)) return true;
        }
        return false;
    }

    /**
     * @param inetAddress
     * @return true if inetAddress is contained in the subnet represented by this instance, otherwise false
     */
    public boolean contains(InetAddress inetAddress) {
        return IPMatcher.match(inetAddress, getSubnetAddress(), getMask() );
    }

    @Override
    public String toString() {
        if (_subnetAddress == null) return "ANY";

        if (_subnetAddress instanceof Inet6Address) {
            return toIPv6String((Inet6Address)_subnetAddress) + "/" + _mask;
        } else {
            return toIPv4String((Inet4Address)_subnetAddress) + "/" + _mask;
        }
    }

}

