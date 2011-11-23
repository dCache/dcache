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
import org.dcache.chimera.nfs.v4.xdr.uint64_t;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.change_info4;
import org.dcache.chimera.nfs.v4.xdr.changeid4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.LINK4resok;
import org.dcache.chimera.nfs.v4.xdr.LINK4res;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.dcache.chimera.posix.AclHandler;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.posix.UnixAcl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationLINK extends AbstractNFSv4Operation {

        private static final Logger _log = LoggerFactory.getLogger(OperationLINK.class);

	OperationLINK(nfs_argop4 args) {
		super(args, nfs_opnum4.OP_LINK);
	}

	@Override
	public nfs_resop4 process(CompoundContext context) {

		_result.oplink = new LINK4res();

		String newName = new String(_args.oplink.newname.value.value.value);

		try {

            Stat parentStat = context.currentInode().statCache();
            UnixAcl acl = new UnixAcl(parentStat.getUid(), parentStat.getGid(),parentStat.getMode() & 0777 );
            if ( ! context.getAclHandler().isAllowed(acl, context.getUser(), AclHandler.ACL_INSERT ) ) {
                throw new ChimeraNFSException( nfsstat4.NFS4ERR_ACCESS, "Permission denied."  );
            }

			context.getFs().createHLink(context.currentInode(),  context.savedInode(),newName );

			_result.oplink.resok4 = new LINK4resok();
			_result.oplink.resok4.cinfo = new change_info4();
			_result.oplink.resok4.cinfo.atomic = true;
			_result.oplink.resok4.cinfo.before = new changeid4( new uint64_t( context.savedInode().statCache().getMTime()));
			_result.oplink.resok4.cinfo.after = new changeid4( new uint64_t( System.currentTimeMillis()) );

			_result.oplink.status = nfsstat4.NFS4_OK;
        }catch(ChimeraNFSException hne){
			_result.oplink.status = hne.getStatus();
		}catch(Exception e) {
			_log.error("LINK: ", e);
		    _result.oplink.status = nfsstat4.NFS4ERR_RESOURCE;
		}
            return _result;

	}

}
