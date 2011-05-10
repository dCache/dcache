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

package org.dcache.xdr;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class netid {

    private netid() {}

    public static String toString(int port) {
        int port_part[] = new int[2];
        port_part[0] = (port & 0xff00) >> 8;
        port_part[1] = port & 0x00ff;
        return "0.0.0.0." + (0xFF & port_part[0]) + "." + (0xFF & port_part[1]);
    }

    public static final InetSocketAddress toInetSocketAddress(String str)
            throws UnknownHostException {

        String[] cb_addr = str.trim().split("[.]");

        byte[] addr = new byte[4];
        addr[0] = (byte) Integer.parseInt(cb_addr[0]);
        addr[1] = (byte) Integer.parseInt(cb_addr[1]);
        addr[2] = (byte) Integer.parseInt(cb_addr[2]);
        addr[3] = (byte) Integer.parseInt(cb_addr[3]);

        InetAddress inetAddr = InetAddress.getByAddress(addr);

        int p1 = Integer.parseInt(cb_addr[4]);
        int p2 = Integer.parseInt(cb_addr[5]);

        int port = (p1 << 8) + p2;

        return new InetSocketAddress(inetAddr, port);

    }

    public static final int getPort(String str) {

        String[] cb_addr = str.trim().split("[.]");

        int p1 = Integer.parseInt(cb_addr[4]);
        int p2 = Integer.parseInt(cb_addr[5]);

        return (p1 << 8) + p2;
    }

    public static int idOf(String id) {
        if("tcp".equals(id)) {
            return IpProtocolType.TCP;
        }else if ("udp".equals(id)) {
            return IpProtocolType.UDP;
        }else{
            return -1;
        }
    }

}
