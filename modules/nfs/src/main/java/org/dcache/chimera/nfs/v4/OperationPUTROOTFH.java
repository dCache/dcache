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

import org.dcache.chimera.nfs.v4.xdr.nfsstat4;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.PUTROOTFH4res;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationPUTROOTFH extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(OperationPUTROOTFH.class);

    public OperationPUTROOTFH(nfs_argop4 op) {
        super(op, nfs_opnum4.OP_PUTROOTFH);
    }

    @Override
    public nfs_resop4 process(CompoundContext context) {

        PUTROOTFH4res res = new PUTROOTFH4res();

        try {
            context.currentInode(FsInode.getRoot(context.getFs()));
            res.status = nfsstat4.NFS4_OK;
        } catch (Exception e) {
            _log.error("PUTROOTFH4:", e);
            res.status = nfsstat4.NFS4ERR_RESOURCE;
        }

        _result.opputrootfh = res;
        return _result;

    }
}
