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

package org.dcache.chimera.nfs.v4;

public interface NFSv4Defaults {

    public static final int NFS4_LEASE_TIME = 90;
    public final static int NFS4_MAXFILENAME = 255;
    // maximal read/write buffer size
    public static final long NFS4_MAXIOBUFFERSIZE = 32768;

    // theoretically, there is no limit on file size
    public final static long NFS4_MAXFILESIZE = Long.MAX_VALUE;

    /**
     * max link count
     */
    public final static int NFS4_MAXLINK = 255;

    // setting the stripe size
    public static final int NFS4_STRIPE_SIZE = (int)NFS4_MAXIOBUFFERSIZE;


    /**
     * NFSv4.1 implementation ID
     */
    public final static String NFS4_IMPLEMENTATION_ID = "Chimera NFSv4.1";

    /**
     * NFSv4.1 implementation domain
     */
    public final static String NFS4_IMPLEMENTATION_DOMAIN = "dCache.ORG";

    /**
     * NFSv4.1 implementation date
     */
    public final static long NFS4_IMPLEMENTATION_DATE = System.currentTimeMillis();

}
