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

import java.security.SecureRandom;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.chimera.nfs.v4.xdr.uint32_t;
import org.dcache.utils.Bytes;

class NFS4State {

    /*
        struct stateid4 {
            uint32_t        seqid;
            opaque          other[12];
        };

       This structure is used for the various state sharing mechanisms
        between the client and server.  For the client, this data structure
        is read-only.  The starting value of the seqid field is undefined.
        The server is required to increment the seqid field monotonically at
        each transition of the stateid.  This is important since the client
        will inspect the seqid in OPEN stateids to determine the order of
        OPEN processing done by the server.

     */

    private final stateid4 _stateid;
    private boolean _isConfimed;

    /**
     * Random generator to generate stateids.
     */
    private static final SecureRandom RANDOM = new SecureRandom();

    public NFS4State(long clientid, int seqid) {

        _stateid = new stateid4();
        _stateid.other = new byte[12];
        _stateid.seqid = new uint32_t(seqid);
        // generated using a cryptographically strong pseudo random number generator.
        Bytes.putLong(_stateid.other, 0, clientid);
        Bytes.putInt(_stateid.other, 8, RANDOM.nextInt());
    }

    public void bumpSeqid() { ++ _stateid.seqid.value; }

    public stateid4 stateid() {
        return _stateid;
    }

    public void confirm() {
    	_isConfimed = true;
    }

    public boolean isConfimed() {
    	return _isConfimed;
    }
}


/*
 * $Log: NFS4State.java,v $
 * Revision 1.4  2006/07/04 15:29:35  tigran
 * fixed  creation of stateid
 *
 * Revision 1.3  2006/07/04 15:17:49  tigran
 * initial stateid provided by client
 *
 * Revision 1.2  2006/07/04 14:46:12  tigran
 * basic state handling
 *
 * Revision 1.1  2006/06/27 16:29:37  tigran
 * first touch to states
 * TODO: it does not work yet!
 *
 */
