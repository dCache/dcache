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
     * @param pattern String representation of a host, either as a hostname glob or an CIDR IP[/subnet] pattern.
     * @param ip InetAddress to be checked for matching of the mask
     * @return true if ip belongs into the fits mask, otherwise false.
     */
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
     * Checks matching ip in specified subnet.
     *
     * @param ip address to test
     * @param subnet address
     * @param mask netmask
     * @return true if ip matches subnet.
     */
    public static boolean match( InetAddress ip, InetAddress subnet, int mask )
    {
        byte[] ipBytes = ip.getAddress();
        byte[] netBytes = subnet.getAddress();

        if (ipBytes.length != netBytes.length) return false;

        int i;
        for (i=0; i<mask/8; i++) {
            if (ipBytes[i] != netBytes[i]) return false;
        }
        return i<ipBytes.length ? ((ipBytes[i]&0xFF) >> (8-mask%8)) == ((netBytes[i]&0xFF) >> (8-mask%8)) : true;
    }
}
