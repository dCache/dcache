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

import java.net.InetSocketAddress;
import org.junit.Test;
import static org.junit.Assert.*;

public class netidTest {

    @Test
    public void testToInetSocketAddress() throws Exception {
        InetSocketAddress expResult = new InetSocketAddress("127.0.0.2", 2052);
        InetSocketAddress result = netid.toInetSocketAddress("127.0.0.2.8.4");
        assertEquals("address decodeing missmatch", expResult, result);
    }

    @Test
    public void testToInetSocketAddressIPv6() throws Exception {
        InetSocketAddress expResult = new InetSocketAddress("0:0:0:0:0:0:0:0", 2052);
        InetSocketAddress result = netid.toInetSocketAddress("0:0:0:0:0:0:0:0.8.4");
        assertEquals("address decodeing missmatch", expResult, result);
    }

    @Test
    public void testGetPort() {
        int port = netid.getPort("127.0.0.2.8.4");
        assertEquals("port decodeing does not match", 2052, port);
    }

}