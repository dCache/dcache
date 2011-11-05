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

import org.dcache.chimera.nfs.v4.xdr.RECLAIM_COMPLETE4res;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfsstat4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationRECLAIM_COMPLETE extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(OperationRECLAIM_COMPLETE.class);

    public OperationRECLAIM_COMPLETE(nfs_argop4 args) {
        super(args, nfs_opnum4.OP_RECLAIM_COMPLETE);
    }

    @Override
    public boolean process(CompoundContext context) {
        _result.opreclaim_complete = new RECLAIM_COMPLETE4res();
        _result.opreclaim_complete.rcr_status = nfsstat4.NFS4ERR_NOTSUPP;
        context.processedOperations().add(_result);
        return false;
    }
}
