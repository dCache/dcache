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

import org.dcache.chimera.nfs.v4.xdr.open_delegation_type4;
import org.dcache.chimera.nfs.v4.xdr.change_info4;
import org.dcache.chimera.nfs.v4.xdr.bitmap4;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.changeid4;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.uint32_t;
import org.dcache.chimera.nfs.v4.xdr.opentype4;
import org.dcache.chimera.nfs.v4.xdr.open_claim_type4;
import org.dcache.chimera.nfs.v4.xdr.open_delegation4;
import org.dcache.chimera.nfs.v4.xdr.uint64_t;
import org.dcache.chimera.nfs.v4.xdr.createmode4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.OPEN4resok;
import org.dcache.chimera.nfs.v4.xdr.OPEN4res;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.dcache.chimera.posix.AclHandler;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.posix.UnixAcl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationOPEN extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(OperationOPEN.class);

    OperationOPEN(nfs_argop4 args) {
        super(args, nfs_opnum4.OP_OPEN);
    }

    @Override
    public nfs_resop4 process(CompoundContext context) {
        OPEN4res res = new OPEN4res();

        try {

            Long clientid = _args.opopen.owner.value.clientid.value.value;
            final NFS4Client client;

            if (context.getSession() == null) {
                client = context.getStateHandler().getClientByID(clientid);

                if (client == null || !client.isConfirmed()) {
                    throw new ChimeraNFSException(nfsstat.NFSERR_STALE_CLIENTID, "bad client id.");
                }

                client.updateLeaseTime();
                _log.debug("open request form clientid: {}, owner: {}",
                        client, new String(_args.opopen.owner.value.owner));
            } else {
                client = context.getSession().getClient();
            }

            res.resok4 = new OPEN4resok();
            res.resok4.attrset = new bitmap4();
            res.resok4.attrset.value = new uint32_t[2];
            res.resok4.attrset.value[0] = new uint32_t(0);
            res.resok4.attrset.value[1] = new uint32_t(0);
            res.resok4.delegation = new open_delegation4();
            res.resok4.delegation.delegation_type = open_delegation_type4.OPEN_DELEGATE_NONE;

            switch (_args.opopen.claim.claim) {

                case open_claim_type4.CLAIM_NULL:

                    if (!context.currentInode().isDirectory()) {
                        throw new ChimeraNFSException(nfsstat.NFSERR_NOTDIR, "not a directory");
                    }

                    String name = NameFilter.convert(_args.opopen.claim.file.value.value.value);
                    _log.debug("regular open for : {}", name);

                    FsInode inode;
                    if (_args.opopen.openhow.opentype == opentype4.OPEN4_CREATE) {

                        boolean exclusive = (_args.opopen.openhow.how.mode == createmode4.EXCLUSIVE4) ||
                                (_args.opopen.openhow.how.mode == createmode4.EXCLUSIVE4_1);

                        try {

                            inode = context.currentInode().inodeOf(name);

                            if (exclusive) {
                                throw new ChimeraNFSException(nfsstat.NFSERR_EXIST, "file already exist");
                            }

                            _log.debug("Opening existing file: {}", name);

                            _log.trace("Check permission");
                            // check file permissions
                            Stat fileStat = inode.statCache();
                            _log.debug("UID  : {}", fileStat.getUid());
                            _log.debug("GID  : {}", fileStat.getGid());
                            _log.debug("Mode : 0{}", Integer.toOctalString(fileStat.getMode() & 0777));
                            UnixAcl fileAcl = new UnixAcl(fileStat.getUid(), fileStat.getGid(), fileStat.getMode() & 0777);
                            if (!context.getAclHandler().isAllowed(fileAcl, context.getUser(), AclHandler.ACL_WRITE)) {
                                throw new ChimeraNFSException(nfsstat.NFSERR_ACCESS, "Permission denied.");
                            }

                            OperationSETATTR.setAttributes(_args.opopen.openhow.how.createattrs, inode, context);
                        } catch (FileNotFoundHimeraFsException he) {

                            // check parent permissions
                            Stat parentStat = context.currentInode().statCache();
                            UnixAcl parentAcl = new UnixAcl(parentStat.getUid(), parentStat.getGid(), parentStat.getMode() & 0777);
                            if (!context.getAclHandler().isAllowed(parentAcl, context.getUser(), AclHandler.ACL_INSERT)) {
                                throw new ChimeraNFSException(nfsstat.NFSERR_ACCESS, "Permission denied.");
                            }

                            _log.debug("Creating a new file: {}", name);
                            inode = context.currentInode().create(name, context.getUser().getUID(),
                                    context.getUser().getGID(), 0600);

                            // FIXME: proper implemtation required
                            switch (_args.opopen.openhow.how.mode) {
                                case createmode4.UNCHECKED4:
                                case createmode4.GUARDED4:
                                    res.resok4.attrset = OperationSETATTR.setAttributes(_args.opopen.openhow.how.createattrs, inode, context);
                                    break;
                                case createmode4.EXCLUSIVE4:
                                case createmode4.EXCLUSIVE4_1:
                            }
                        }

                    } else {

                        inode = context.currentInode().inodeOf(name);

                        Stat inodeStat = inode.statCache();
                        UnixAcl fileAcl = new UnixAcl(inodeStat.getUid(), inodeStat.getGid(), inodeStat.getMode() & 0777);
                        if (!context.getAclHandler().isAllowed(fileAcl, context.getUser(), AclHandler.ACL_READ)) {
                            throw new ChimeraNFSException(nfsstat.NFSERR_ACCESS, "Permission denied.");
                        }

                        if (inode.isDirectory()) {
                            throw new ChimeraNFSException(nfsstat.NFSERR_ISDIR, "path is a directory");
                        }

                        if (inode.isLink()) {
                            throw new ChimeraNFSException(nfsstat.NFSERR_SYMLINK, "path is a symlink");
                        }
                    }

                    context.currentInode(inode);

                    break;
                case open_claim_type4.CLAIM_PREVIOUS:
                    _log.debug("open by Inode for : {}", context.currentInode());
                    break;
                case open_claim_type4.CLAIM_DELEGATE_CUR:
                    break;
                case open_claim_type4.CLAIM_DELEGATE_PREV:
                    break;
            }

            res.resok4.cinfo = new change_info4();
            res.resok4.cinfo.atomic = true;
            res.resok4.cinfo.before = new changeid4(new uint64_t(context.currentInode().statCache().getMTime()));
            res.resok4.cinfo.after = new changeid4(new uint64_t(System.currentTimeMillis()));

            /*
             * if it's not session-based  request, then client have to confirm
             */
            if (context.getSession() == null) {
                res.resok4.rflags = new uint32_t(nfs4_prot.OPEN4_RESULT_LOCKTYPE_POSIX
                        | nfs4_prot.OPEN4_RESULT_CONFIRM);
            } else {
                res.resok4.rflags = new uint32_t(nfs4_prot.OPEN4_RESULT_LOCKTYPE_POSIX);
            }

            NFS4State nfs4state = client.createState();
            res.resok4.stateid = nfs4state.stateid();

            _log.debug("New stateID: {}", nfs4state.stateid());

            res.status = nfsstat.NFS_OK;

        } catch (ChimeraNFSException he) {
            _log.debug("OPEN:", he.getMessage());
            res.status = he.getStatus();
        } catch (FileExistsChimeraFsException e) {
            _log.debug("OPEN: {}", e.getMessage());
            res.status = nfsstat.NFSERR_EXIST;
        } catch (FileNotFoundHimeraFsException fnf) {
            _log.debug("OPEN: {}", fnf.getMessage());
            res.status = nfsstat.NFSERR_NOENT;
        } catch (Exception hfe) {
            _log.error("OPEN:", hfe);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        }

        _result.opopen = res;
        return _result;

    }
}
