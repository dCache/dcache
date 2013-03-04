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
package org.dcache.chimera.nfs;

import javax.security.auth.Subject;

import org.dcache.auth.Subjects;
import org.dcache.chimera.posix.UnixUser;
import org.dcache.xdr.RpcCall;

/**
 * Utility class extract user record from NFS request
 */
public class NfsUser {

    private static final UnixUser NOBODY = new UnixUser(-1, -1, new int[0], "localhost");

    /*no instances allowed*/
    private NfsUser() {
    }

    public static UnixUser remoteUser(RpcCall call, ExportFile exports) {

        int uid;
        int gid;
        int[] gids;

        Subject subject = call.getCredential().getSubject();
        if( subject.equals(Subjects.NOBODY) ) {
            return NOBODY;
        }

        uid = (int)Subjects.getUid(subject);
        gids = from(Subjects.getGids(subject));
        gid = gids.length > 0 ? gids[0] : -1;

        String host = call.getTransport().getRemoteSocketAddress().getAddress().getHostAddress();

        // root access only for trusted hosts
        if (uid == 0) {
            if ((exports == null) || !exports.isTrusted(
                    call.getTransport().getRemoteSocketAddress().getAddress())) {

                // FIXME: actual 'nobody' account should be used
                uid = -1;
                gid = -1;
            }
        }

        return  new UnixUser(uid, gid, gids, host);
    }

    private static int[] from(long[] longs) {
        int[] ints = new int[longs.length];

        int i = 0;
        for (long l : longs) {
            ints[i] = (int) l;
            i++;
        }
        return ints;
    }

}
