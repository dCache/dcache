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

import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.PUTFH4res;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;

public class OperationPUTFH extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(OperationPUTFH.class);

    public OperationPUTFH(nfs_argop4 args) {
        super(args, nfs_opnum4.OP_PUTFH);
    }

    @Override
    public nfs_resop4 process(CompoundContext context) {
        PUTFH4res res = new PUTFH4res();

        try {
            context.currentInode(context.getFs().inodeFromBytes(_args.opputfh.object.value));
            _log.debug("NFS Request  PUTFH4 current: {}", context.currentInode());
            res.status = nfsstat.NFS_OK;
        } catch (IllegalArgumentException iae) {
            res.status = nfsstat.NFSERR_BADHANDLE;
        } catch (FileNotFoundHimeraFsException e) {
            res.status = nfsstat.NFSERR_BADHANDLE;
        } catch (Exception e) {
            _log.error("PUTFH4:", e);
            res.status = nfsstat.NFSERR_RESOURCE;
        }

        _result.opputfh = res;
        return _result;
    }
}
