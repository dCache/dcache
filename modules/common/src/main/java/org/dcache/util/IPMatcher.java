/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package org.dcache.util;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.InetAddresses.forString;
import static com.google.common.net.InetAddresses.getEmbeddedIPv4ClientAddress;
import static com.google.common.primitives.Ints.fromByteArray;
import static com.google.common.primitives.Longs.fromBytes;

public class IPMatcher {
    private static final Pattern IPV4_PATTERN = Pattern.compile("((?:\\d{1,3}\\.){3}\\d{1,3})(?:/((?:(?:\\d{1,3}\\.){3}\\d{1,3})|\\d{1,2}))?");
    private static final Pattern IPV6_PATTERN = Pattern.compile("((?:::)|(?:(?:(?:[0-9a-fA-F]{1,4}:)|:){1,7})[0-9a-fA-F]{1,4})(?:/(\\d{1,3}))?");

    private static final int HOST_IP_GROUP_INDEX   = 1;
    private static final int MASK_BITS_GROUP_INDEX = 2;

    private static final int IPv4_FULL_MASK = 32;
    private static final int IPv6_FULL_MASK = 128;
    private static final int IPv6_HALF_MASK = 64;

    static NetworkReality networkReality = new NetworkReality();

    public static int convertIPv4MaskStringToCidr(String maskString) {
        int mask;
        if (maskString.contains(".")) {
            mask = fromByteArray(forString(maskString).getAddress());
            mask = IPv4_FULL_MASK - Integer.numberOfTrailingZeros(mask);
        } else {
            mask = Integer.parseInt(maskString);
        }
        return mask;
    }

    /**
     * Returns the subnet part of an InetAddress according to a mask in CIDR notation
     *
     * Example: inetAddress=123.123.45.67, mask=16 will return an InetAddress of 123.123.0.0
     *
     * @param inetAddress base address
     * @param mask mask in CIDR notation
     * @return
     * @throws UnknownHostException will be thrown if the resulting InetAddress is not valid.
     * This should not happen since the base address will always be valid.
     */
    public static InetAddress maskInetAddress(InetAddress inetAddress, int mask) throws UnknownHostException {
        byte[] address = inetAddress.getAddress();

        if (mask == 0) {
            return InetAddress.getByAddress(new byte[address.length]);
        }

        if (mask%8 != 0) {
            address[mask / 8] = (byte) (address[mask / 8] & (0xff << (8 - mask % 8)));
        }
        for (int i=mask/8+1; i<address.length; i++) {
            address[i] = 0;
        }

        return InetAddress.getByAddress(address);
    }

    /**
     * Matches the hostname of an InetAddress with a glob.
     *
     * Example: *.example.org will match host1.example.org and www.host2.example.org
     *
     * Warning: We stronly advise you not to use host globs for security relevant methods
     * as they may easily introduce security holes.
     *
     * Example: Given you have 5 hosts (foo1, foo11, foo1a, foo-b and foofoo) that you
     * want to allow access to. If you specify to allow all hosts that match foo* also
     * for example foo.bogus.com will be allowed.
     *
     * @param hostGlob glob notation of a hostname pattern
     * @param inetAddress address to be matched
     * @return
     */
    public static boolean matchHostGlob(InetAddress inetAddress, String hostGlob) {
        return new Glob(hostGlob).matches(networkReality.getHostNameFor(inetAddress));
    }

    /**
     * Matches an InetAddress with the CIDR notation of a subnet.
     *
     * @param cidrPattern CIDR notation of the subnet
     * @param inetAddress address to be matched
     * @return
     */
    public static boolean matchCidrPattern(InetAddress inetAddress, String cidrPattern) {
        return Subnet.create(cidrPattern).contains(inetAddress);
    }

    /**
     * @param hostname Hostname to be matched with the subnet defined by subnet and mask
     * @param subnet subnet address
     * @param mask netmask in CIDR notation
     * @return true if any of the IPs assigned to the host specified by hostname lies iwthin the subnet, otherwise false
     * @throws UnknownHostException
     */
    public static boolean matchHostname(String hostname, InetAddress subnet, int mask) throws UnknownHostException {
        return matchAny(InetAddress.getAllByName(hostname), subnet, mask);
    }

    /**
     * @param ips array of ips to be matched with the subnet defined by subnet and mask
     * @param subnet subnet address
     * @param mask netmask in CIDR notation
     * @return true if any of the IPs in ips falls lies the subnet, otherwise false
     */
    public static boolean matchAny(InetAddress[] ips, InetAddress subnet, int mask) {
        for (InetAddress inetAddress : ips) {
            if (match(inetAddress, subnet, mask)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks matching ip in specified subnet.
     *
     * @param ip address to test
     * @param subnet address
     * @param mask netmask in CIDR notation
     * @return true if ip matches subnet.
     */
    public static boolean match(InetAddress ip, InetAddress subnet, int mask) {

        checkArgument(mask >= 0, "Netmask should be positive");

        if(mask == 0) {
            return true; // match all
        }

        byte[] ipBytes = ip.getAddress();
        byte[] netBytes = subnet.getAddress();

        if (ipBytes.length != netBytes.length) {
            return false;
        }

        if (ipBytes.length == 4) {
            checkArgument(mask <= IPv4_FULL_MASK,
                    "Netmask for IPv4 can't be bigger than" + IPv4_FULL_MASK);
            /*
             * IPv4 can be represented as a 32 bit ints.
             */
            int ipAsInt = fromByteArray(ipBytes);
            int netAsBytes = fromByteArray(netBytes);

            return (ipAsInt ^ netAsBytes) >> (IPv4_FULL_MASK - mask) == 0;
        }

        checkArgument(mask <= IPv6_FULL_MASK,
                "Netmask for IPv6 can't be bigger than" + IPv6_FULL_MASK);

        /*
         * IPv6 can be represented as two 64 bit longs.
         *
         * We evaluate second long only if bitmask bigger than 64. The second
         * longs are created only if needed as it turned to be the slowest part.
         */
        long ipAsLong0 = fromBytes(ipBytes[0], ipBytes[1], ipBytes[2], ipBytes[3],
                ipBytes[4], ipBytes[5], ipBytes[6], ipBytes[7]);
        long netAsLong0 = fromBytes(netBytes[0], netBytes[1], netBytes[2], netBytes[3],
                netBytes[4], netBytes[5], netBytes[6], netBytes[7]);

        if (mask > 64) {
            long ipAsLong1 = fromBytes(ipBytes[8], ipBytes[9], ipBytes[10], ipBytes[11],
                    ipBytes[12], ipBytes[13], ipBytes[14], ipBytes[15]);

            long netAsLong1 = fromBytes(netBytes[8], netBytes[9], netBytes[10], netBytes[11],
                    netBytes[12], netBytes[13], netBytes[14], netBytes[15]);

            return (ipAsLong0 == netAsLong0)
                    & (ipAsLong1 ^ netAsLong1) >> (IPv6_FULL_MASK - mask) == 0;
        }
        return (ipAsLong0 ^ netAsLong0) >> (IPv6_HALF_MASK - mask) == 0;
    }


    public static InetAddress convertToIPv4IfPossible(InetAddress inetAddress) {
        if (!(inetAddress instanceof Inet6Address)) {
            return inetAddress;
        }

        try {
            return getEmbeddedIPv4ClientAddress((Inet6Address)inetAddress);
        } catch (IllegalArgumentException illegalArgumentException) {}

        return inetAddress;
    }
}
