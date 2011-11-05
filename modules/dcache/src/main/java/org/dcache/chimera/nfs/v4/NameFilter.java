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

import org.dcache.chimera.nfs.v4.xdr.*;
import org.dcache.chimera.nfs.ChimeraNFSException;

class NameFilter {

    /* utility calls */
    private NameFilter(){}

    /**
     *
     * validate name and return an instance of string or throw exception
     *
     * @param bytes
     * @return
     * @throws ChimeraNFSException
     */
    public static String convert(byte[] bytes) throws ChimeraNFSException {

        String ret = null;

        if (bytes.length == 0) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_INVAL, "zero-length name");
        }

        if (bytes.length > NFSv4Defaults.NFS4_MAXFILENAME) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_NAMETOOLONG, "file name too long");
        }

        try {
            ret = new String(bytes, "UTF-8");
        } catch (Exception e) {
            throw new ChimeraNFSException(nfsstat4.NFS4ERR_INVAL, "invalid utf8 name");
        }

        return ret;
    }
}
