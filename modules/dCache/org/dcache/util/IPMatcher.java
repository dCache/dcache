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

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.InetAddresses.getEmbeddedIPv4ClientAddress;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPMatcher {

    private static final Pattern IPV4_PATTERN = Pattern.compile("((?:\\d{1,3}\\.){3}\\d{1,3})(?:/(\\d{1,2}))?");
    private static final Pattern IPV6_PATTERN = Pattern.compile("((?:::)|(?:(?:(?:[0-9a-fA-F]{1,4}:)|:){1,7})[0-9a-fA-F]{1,4})(?:/(\\d{1,3}))?");

    private static final int HOST_IP_GROUP_INDEX   = 1;
    private static final int MASK_BITS_GROUP_INDEX = 2;

    /**
     * Matches an IP with the string representation of a host name.
     *
     * This method is deprecated. Please use one of the following new methods:
     * - matchHostGlob
     * - matchCidrPattern
     * They are also more powerful and support a wider range of ipv6 features.
     *
     * @param pattern String representation of a host, either as a hostname glob or an CIDR IP[/subnet] pattern.
     * @param ip InetAddress to be checked for matching of the mask
     * @return true if ip belongs into the fits mask, otherwise false.
     */
    @Deprecated
    public static boolean match(String pattern, InetAddress ip)
    {
        try {
            Matcher matcher;
            if ((matcher = IPV4_PATTERN.matcher(pattern)).matches()) {
                String netmaskGroup = matcher.group(MASK_BITS_GROUP_INDEX);
                int netmask = isNullOrEmpty(netmaskGroup) ? 32 : Integer.parseInt(netmaskGroup);
                return match(InetAddress.getByName(matcher.group(HOST_IP_GROUP_INDEX)), ip, netmask);
            } else if ((matcher = IPV6_PATTERN.matcher(pattern)).matches()) {
                String netmaskGroup = matcher.group(MASK_BITS_GROUP_INDEX);
                int netmask = isNullOrEmpty(netmaskGroup) ? 128 : Integer.parseInt(netmaskGroup);
                return match(InetAddress.getByName(matcher.group(HOST_IP_GROUP_INDEX)), ip, netmask);
            }

            String hostname = ip.getHostName();
            return new Glob(pattern).matches(hostname);

        } catch (UnknownHostException unknownHostException) {
            return false;
        }
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

        if (mask == 0) return InetAddress.getByAddress(new byte[address.length]);

        if (mask%8 != 0) address[mask/8] = (byte) (address[mask/8] & (0xff << (8-mask%8)));
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
        return new Glob(hostGlob).matches(inetAddress.getHostName());
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
            if (match(inetAddress, subnet, mask)) return true;
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
    public static boolean match(InetAddress ip, InetAddress subnet, int mask)
    {
        if (mask == 0) return true;

        byte[] ipBytes = convertToIPv4IfPossible(ip).getAddress();
        byte[] netBytes = convertToIPv4IfPossible(subnet).getAddress();

        if (ipBytes.length != netBytes.length) return false;

        int i;
        for (i=0; i<(mask%(netBytes.length*8))/8; i++) {
            if (ipBytes[i] != netBytes[i]) return false;
        }
        return i<ipBytes.length ? ((ipBytes[i]&0xFF) >> (8-mask%8)) == ((netBytes[i]&0xFF) >> (8-mask%8)) : true;
    }


    public static InetAddress convertToIPv4IfPossible(InetAddress inetAddress) {
        if (inetAddress instanceof Inet4Address) return inetAddress;

        try {
            return getEmbeddedIPv4ClientAddress((Inet6Address)inetAddress);
        } catch (IllegalArgumentException illegalArgumentException) {}

        return inetAddress;
    }

}
