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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.SAVEFH4res;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;

public class OperationSAVEFH extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(OperationSAVEFH.class);

    OperationSAVEFH(nfs_argop4 args) {
        super(args, nfs_opnum4.OP_SAVEFH);
    }

    @Override
    public nfs_resop4 process(CompoundContext context) {
        SAVEFH4res res = new SAVEFH4res();

        try {
            context.saveCurrentInode();
            res.status = nfsstat.NFS_OK;
        } catch (ChimeraNFSException he) {
            _log.trace("SAVEFH4: {}", he.getMessage());
            res.status = he.getStatus();
        } catch (Exception e) {
            _log.error("SAVEFH4:", e);
            res.status = nfsstat.NFSERR_RESOURCE;
        }

        _result.opsavefh = res;
        return _result;

    }
}
