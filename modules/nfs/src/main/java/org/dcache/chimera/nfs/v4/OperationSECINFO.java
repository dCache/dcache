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
import org.dcache.chimera.nfs.v4.xdr.SECINFO4res;
import org.dcache.chimera.nfs.v4.xdr.SECINFO4resok;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.dcache.chimera.nfs.v4.xdr.rpcsec_gss_info;
import org.dcache.chimera.nfs.v4.xdr.secinfo4;
import org.dcache.xdr.RpcAuthType;

public class OperationSECINFO extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(OperationSECINFO.class);

    OperationSECINFO(nfs_argop4 args) {
        super(args, nfs_opnum4.OP_SECINFO);
    }

    @Override
    public nfs_resop4 process(CompoundContext context) {

        SECINFO4res res = new SECINFO4res();

        try {

            if (!context.currentInode().isDirectory()) {
                throw new ChimeraNFSException(nfsstat.NFSERR_NOTDIR, "not a directory");
            }

            res.resok4 = new SECINFO4resok();
            res.resok4.value = new secinfo4[1];

            res.resok4.value[0] = new secinfo4();
            res.resok4.value[0].flavor = RpcAuthType.UNIX;
            res.resok4.value[0].flavor_info = new rpcsec_gss_info();
            context.clearCurrentInode();
            res.status = nfsstat.NFS_OK;
        } catch (ChimeraNFSException he) {
            _log.debug("SECINFO:", he.getMessage());
            res.status = he.getStatus();
        }

        _result.opsecinfo = res;
        return _result;

    }
}
