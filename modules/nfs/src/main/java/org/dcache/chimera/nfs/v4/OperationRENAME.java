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

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.RENAME4res;
import org.dcache.chimera.nfs.v4.xdr.RENAME4resok;
import org.dcache.chimera.nfs.v4.xdr.change_info4;
import org.dcache.chimera.nfs.v4.xdr.changeid4;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.dcache.chimera.nfs.v4.xdr.uint64_t;
import org.dcache.chimera.posix.AclHandler;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.posix.UnixAcl;

public class OperationRENAME extends AbstractNFSv4Operation {

        private static final Logger _log = LoggerFactory.getLogger(OperationRENAME.class);

	OperationRENAME(nfs_argop4 args) {
		super(args, nfs_opnum4.OP_RENAME);
	}

	@Override
	public nfs_resop4 process(CompoundContext context) {
    	RENAME4res res = new RENAME4res();

    	try {

    		FsInode sourceDir = context.savedInode();
    		FsInode destDir = context.currentInode();

            if( ! sourceDir.isDirectory() ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_NOTDIR, "source path not a directory");
            }

            if( ! destDir.isDirectory() ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_NOTDIR, "destination path  not a directory");
            }

            String oldName = NameFilter.convert (_args.oprename.oldname.value.value.value);
            String newName = NameFilter.convert (_args.oprename.newname.value.value.value);

            if( oldName.length() == 0 ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_INVAL, "zero-length name");
            }

            if( newName.length() == 0 ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_INVAL, "zero-length name");
            }


            if( oldName.equals(".") || oldName.equals("..") ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_BADNAME, "bad name '.' or '..'");
            }

            if( newName.equals(".") || newName.equals("..") ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_BADNAME, "bad name '.' or '..'");
            }


            if( sourceDir.fsId() != destDir.fsId() ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_XDEV, "cross filesystem request");
            }

            _log.debug("Rename: src={} name={} dest={} name={}", sourceDir, oldName, destDir, newName);

            Stat fromStat = sourceDir.stat();
            Stat toStat = destDir.stat();
            UnixAcl fromAcl = new UnixAcl(fromStat.getUid(), fromStat.getGid(), fromStat.getMode() & 0777);
            UnixAcl toAcl = new UnixAcl(toStat.getUid(), toStat.getGid(), toStat.getMode() & 0777);
            if (!(context.getAclHandler().isAllowed(fromAcl, context.getUser(), AclHandler.ACL_DELETE)
                    && context.getAclHandler().isAllowed(toAcl, context.getUser(), AclHandler.ACL_INSERT))) {
                throw new ChimeraNFSException(nfsstat.NFSERR_ACCESS, "Permission denied.");
            }

            context.getFs().move(sourceDir, oldName, destDir, newName);

            res.resok4 = new RENAME4resok();

            res.resok4.source_cinfo = new change_info4();
            res.resok4.source_cinfo.atomic = true;
            res.resok4.source_cinfo.before = new changeid4( new uint64_t(sourceDir.statCache().getMTime()));
            res.resok4.source_cinfo.after = new changeid4( new uint64_t( System.currentTimeMillis()) );

            res.resok4.target_cinfo = new change_info4();
            res.resok4.target_cinfo.atomic = true;
            res.resok4.target_cinfo.before = new changeid4( new uint64_t(sourceDir.statCache().getMTime()));
            res.resok4.target_cinfo.after = new changeid4( new uint64_t( System.currentTimeMillis()) );


            res.status = nfsstat.NFS_OK;

    	}catch(FileNotFoundHimeraFsException fnf) {
    		res.status = nfsstat.NFSERR_NOENT;
        }catch(ChimeraNFSException he) {
            _log.error("RENAME: {}", he.getMessage());
            res.status = he.getStatus();
        } catch (ChimeraFsException hfe) {
            res.status = nfsstat.NFSERR_SERVERFAULT;
            _log.error("RENAME: {}", hfe.getMessage());
        }


       _result.oprename = res;
            return _result;
	}

}
