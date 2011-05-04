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

package org.dcache.xdr.portmap;

public class OncRpcPortmap {

    /**
     * portmap/rpcbing program number as defined in rfc1833.
     */
    public static final int PORTMAP_PROGRAMM = 100000;

    /**
     * portmap/rpcbing TCP/UDP port number as defined in rfc1833.
     */

    public static final int PORTMAP_PORT = 111;
    /*
     * V2
     */
    public static final int PORTMAP_V2 = 2;
    public static final int PMAPPROC_NULL = 0;
    public static final int PMAPPROC_SET = 1;
    public static final int PMAPPROC_UNSET = 2;
    public static final int PMAPPROC_GETPORT = 3;
    public static final int PMAPPROC_DUMP = 4;
    public static final int PMAPPROC_CALLIT = 5;

    /*
     * V4
     */
    public static final int PORTMAP_V4 = 4;
    public static final int RPCBPROC_SET = 1;
    public static final int RPCBPROC_UNSET = 2;
    public static final int RPCBPROC_GETADDR = 3;
    public static final int RPCBPROC_DUMP = 4;
    public static final int RPCBPROC_GETTIME = 6;
    public static final int RPCBPROC_UADDR2TADDR = 7;
    public static final int RPCBPROC_TADDR2UADDR = 8;
    public static final int RPCBPROC_GETVERSADDR = 9;
    public static final int RPCBPROC_INDIRECT = 10;
    public static final int RPCBPROC_GETADDRLIST = 11;
    public static final int RPCBPROC_GETSTAT = 12;
}
