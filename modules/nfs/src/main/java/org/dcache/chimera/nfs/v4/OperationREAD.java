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

import java.nio.ByteBuffer;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.IOHimeraFsException;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.READ4res;
import org.dcache.chimera.nfs.v4.xdr.READ4resok;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.dcache.chimera.posix.AclHandler;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.posix.UnixAcl;

public class OperationREAD extends AbstractNFSv4Operation {

        private static final Logger _log = LoggerFactory.getLogger(OperationREAD.class);

	public OperationREAD(nfs_argop4 args) {
		super(args, nfs_opnum4.OP_READ);
	}

	@Override
	public nfs_resop4 process(CompoundContext context) {
        READ4res res = new READ4res();


        try {

            if( context.currentInode().isDirectory() ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_ISDIR, "path is a directory");
            }

            if( context.currentInode().isLink() ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_INVAL, "path is a symlink");
            }

            Stat inodeStat = context.currentInode().statCache();

            UnixAcl fileAcl = new UnixAcl(inodeStat.getUid(), inodeStat.getGid(),inodeStat.getMode() & 0777 );
            if ( ! context.getAclHandler().isAllowed(fileAcl, context.getUser(), AclHandler.ACL_READ)  ) {
                throw new ChimeraNFSException( nfsstat.NFSERR_ACCESS, "Permission denied."  );
            }

            if(context.getMinorversion() == 0) {
                /*
                 * The NFSv4.0 spec requires to update lease time as long as
                 * client needs the file. This is done through  READ, WRITE
                 * and RENEW opertations. With introduction of sessions in
                 * v4.1 update of the lease time done through SEQUENCE operation.
                 */
                context.getStateHandler().updateClientLeaseTime(_args.opread.stateid);
            }


            long offset = _args.opread.offset.value.value;
            int count = _args.opread.count.value.value;

            ByteBuffer buf = ByteBuffer.allocate(count);

            int bytesReaded = context.currentInode().read(offset, buf.array(), 0, count);
            if( bytesReaded < 0 ) {
                throw new IOHimeraFsException("IO not allowd");
            }

            /*
             * While we have written directly into back-end byte array
             * tell the byte buffer the actual position.
             */
            buf.position(bytesReaded);

            res.status = nfsstat.NFS_OK;
            res.resok4 = new READ4resok();

            res.resok4.data = buf;

            if( offset + bytesReaded >= inodeStat.getSize() ) {
                res.resok4.eof = true;
            }

        }catch(IOHimeraFsException hioe) {
            _log.trace("READ: {}", hioe.getMessage() );
            res.status = nfsstat.NFSERR_IO;
        }catch(ChimeraNFSException he) {
            _log.trace("READ: {}", he.getMessage() );
            res.status = he.getStatus();
        }catch(ChimeraFsException hfe) {
            res.status = nfsstat.NFSERR_NOFILEHANDLE;
        }


       _result.opread = res;
            return _result;
	}

}
