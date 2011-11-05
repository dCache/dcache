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
package org.dcache.utils.net;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import com.google.common.net.InetAddresses;
/**
 * Utility class for InetSocketAddress manipulations.
 * @author tigran
 */
public class InetSocketAddresses {

    /* utility class. No instances are allowed */
    private InetSocketAddresses() {}

    /**
     * Convert UADDR string into {@link InetSocketAddress} as defined in rfc5665.
     * <pre>
     * IPv4 format:
     *     h1.h2.h3.h4.p1.p2
     *
     * The prefix "h1.h2.h3.h4" is the standard textual form for
     * representing an IPv4 address, which is always four octets long.
     * Assuming big-endian ordering, h1, h2, h3, and h4 are, respectively,
     * the first through fourth octets each converted to ASCII-decimal.  The
     * suffix "p1.p2" is a textual form for representing a service port.
     * Assuming big-endian ordering, p1 and p2 are, respectively, the first
     * and second octets each converted to ASCII-decimal.  For example, if a
     * host, in big-endian order, has an address in hexadecimal of
     * 0xC0000207 and there is a service listening on, in big-endian order,
     * port 0xCB51 (decimal 52049), then the complete uaddr is
     * "192.0.2.7.203.81".
     *
     * IPv6:
     *     x1:x2:x3:x4:x5:x6:x7:x8.p1.p2
     * The suffix "p1.p2" is the service port, and is computed the same way
     * as with uaddrs for transports over IPv4 (see Section 5.2.3.3).  The
     * prefix "x1:x2:x3:x4:x5:x6:x7:x8" is the preferred textual form for
     * representing an IPv6 address as defined in Section 2.2 of RFC 4291
     * Additionally, the two alternative forms specified in Section 2.2
     * of RFC 4291 are also acceptable.
     * </pre>
     * @param address
     * @return socket address
     */
    public static InetSocketAddress forUaddrString(String uaddr) {

        int secondPort = uaddr.lastIndexOf('.');
        if( secondPort == -1 ) {
            throw new IllegalArgumentException("address " + uaddr + " doesn't match rfc5665");
        }

        int firstPort = uaddr.lastIndexOf('.', secondPort -1);
        if( secondPort == -1 ) {
            throw new IllegalArgumentException("address " + uaddr + " doesn't match rfc5665");
        }

        InetAddress inetAddr = InetAddresses.forString(uaddr.substring(0, firstPort));

        int p1 = Integer.parseInt(uaddr.substring(firstPort +1, secondPort));
        int p2 = Integer.parseInt(uaddr.substring(secondPort +1));

        int port = (p1 << 8) + p2;

        return new InetSocketAddress(inetAddr, port);
    }

    /**
     * Convert a {@link String} in a form <code>host:port</code>
     * into corresponding {@link InetSocketAddress}.
     * @param address
     * @return socketAddress
     */
    public static InetSocketAddress inetAddressOf(String address) {
        int colom = address.indexOf(":");
        if (colom < 0) {
            throw new IllegalArgumentException("invalid host:port format");
        }

        return new InetSocketAddress(address.substring(0, colom),
                Integer.parseInt(address.substring(colom + 1)));
    }

    /**
     * Convert {@link InetSocketAddress} to it's UADDR representation as defined in rfc5665.
     * @param socketAddress
     * @return uaddr.
     */
    public static String uaddrOf(InetSocketAddress socketAddress) {

        int port = socketAddress.getPort();
        int port_part[] = new int[2];
        port_part[0] = (port & 0xff00) >> 8;
        port_part[1] = port & 0x00ff;
        return socketAddress.getAddress().getHostAddress() +
                "." + port_part[0] + "." + port_part[1];
    }

    /**
     * Convert <code>hostname</code> and <code>port</code> into UADDR representation
     * as defined in rfc5665.
     * @param host
     * @param port
     * @return uaddr
     */
    public static String uaddrOf(String host, int port) {
        return uaddrOf( new InetSocketAddress(host, port));
    }
}
