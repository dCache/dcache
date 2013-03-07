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
import org.dcache.chimera.nfs.v4.xdr.ACCESS4res;
import org.dcache.chimera.nfs.v4.xdr.ACCESS4resok;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.dcache.chimera.nfs.v4.xdr.uint32_t;
import org.dcache.chimera.posix.AclHandler;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.posix.UnixAcl;

public class OperationACCESS extends AbstractNFSv4Operation {

        private static final Logger _log = LoggerFactory.getLogger(OperationACCESS.class);

	OperationACCESS(nfs_argop4 args) {
		super(args, nfs_opnum4.OP_ACCESS);
	}

	@Override
	public nfs_resop4 process(CompoundContext context) {

        ACCESS4res res = new ACCESS4res();

        _log.trace("NFS Request ACCESS uid: {}", context.getUser() );

        try {
            int reqAccess = _args.opaccess.access.value;
            Stat objStat = context.currentInode().statCache();
            UnixAcl acl = new UnixAcl(objStat.getUid(), objStat.getGid(),objStat.getMode() & 0777 );

            int realAccess = 0;


            if( (reqAccess & nfs4_prot.ACCESS4_EXECUTE) == nfs4_prot.ACCESS4_EXECUTE ) {
                if (  context.getAclHandler().isAllowed(acl, context.getUser(), AclHandler.ACL_EXECUTE ) ) {
                    realAccess |= nfs4_prot.ACCESS4_EXECUTE;
                }
            }

            if( (reqAccess & nfs4_prot.ACCESS4_EXTEND) == nfs4_prot.ACCESS4_EXTEND ) {
                if (  context.getAclHandler().isAllowed(acl, context.getUser(), AclHandler.ACL_INSERT ) ) {
                    realAccess |= nfs4_prot.ACCESS4_EXTEND;
                }
            }

            if( (reqAccess & nfs4_prot.ACCESS4_LOOKUP) == nfs4_prot.ACCESS4_LOOKUP ) {
                if (  context.getAclHandler().isAllowed(acl, context.getUser(), AclHandler.ACL_LOOKUP ) ) {
                    realAccess |= nfs4_prot.ACCESS4_LOOKUP;
                }
            }

            if( (reqAccess & nfs4_prot.ACCESS4_DELETE) == nfs4_prot.ACCESS4_DELETE ) {
                if (  context.getAclHandler().isAllowed(acl, context.getUser(), AclHandler.ACL_DELETE ) ) {
                    realAccess |= nfs4_prot.ACCESS4_DELETE;
                }
            }

            if( (reqAccess & nfs4_prot.ACCESS4_MODIFY) == nfs4_prot.ACCESS4_MODIFY ) {
                if (  context.getAclHandler().isAllowed(acl, context.getUser(), AclHandler.ACL_WRITE ) ){
                    realAccess |= nfs4_prot.ACCESS4_MODIFY;
                }
            }

            if( (reqAccess & nfs4_prot.ACCESS4_READ) == nfs4_prot.ACCESS4_READ ) {
                if (  context.getAclHandler().isAllowed(acl, context.getUser(), AclHandler.ACL_READ ) ) {
                    realAccess |= nfs4_prot.ACCESS4_READ;
                }
            }

            res.resok4 = new ACCESS4resok();
            res.resok4.access = new uint32_t( realAccess );
            res.resok4.supported = new uint32_t( realAccess );

            res.status = nfsstat.NFS_OK;
        }catch(ChimeraNFSException he) {
            _log.trace("ACCESS: {}", he.getMessage() );
            res.status = he.getStatus();
        }catch(Exception e) {
            _log.error("ACCESS:", e);
            res.status = nfsstat.NFSERR_RESOURCE;
        }

        _result.opaccess = res;
            return _result;

	}

}
