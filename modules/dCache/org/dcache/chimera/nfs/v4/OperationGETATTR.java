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

import org.dcache.chimera.nfs.v4.xdr.fattr4_numlinks;
import org.dcache.chimera.nfs.v4.xdr.fattr4_hidden;
import org.dcache.chimera.nfs.v4.xdr.fattr4_system;
import org.dcache.chimera.nfs.v4.xdr.fattr4_aclsupport;
import org.dcache.chimera.nfs.v4.xdr.nfs_ftype4;
import org.dcache.chimera.nfs.v4.xdr.attrlist4;
import org.dcache.chimera.nfs.v4.xdr.fattr4_case_insensitive;
import org.dcache.chimera.nfs.v4.xdr.nfs_lease4;
import org.dcache.chimera.nfs.v4.xdr.nfs_fh4;
import org.dcache.chimera.nfs.v4.xdr.fattr4_rawdev;
import org.dcache.chimera.nfs.v4.xdr.utf8string;
import org.dcache.chimera.nfs.v4.xdr.fattr4_maxname;
import org.dcache.chimera.nfs.v4.xdr.fattr4_owner;
import org.dcache.chimera.nfs.v4.xdr.fattr4_space_used;
import org.dcache.chimera.nfs.v4.xdr.fattr4_maxlink;
import org.dcache.chimera.nfs.v4.xdr.fattr4_unique_handles;
import org.dcache.chimera.nfs.v4.xdr.fattr4_lease_time;
import org.dcache.chimera.nfs.v4.xdr.uint64_t;
import org.dcache.chimera.nfs.v4.xdr.fattr4_fh_expire_type;
import org.dcache.chimera.nfs.v4.xdr.int64_t;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfsace4;
import org.dcache.chimera.nfs.v4.xdr.fattr4_named_attr;
import org.dcache.chimera.nfs.v4.xdr.specdata4;
import org.dcache.chimera.nfs.v4.xdr.bitmap4;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.fattr4_homogeneous;
import org.dcache.chimera.nfs.v4.xdr.uint32_t;
import org.dcache.chimera.nfs.v4.xdr.layouttype4;
import org.dcache.chimera.nfs.v4.xdr.fattr4_maxread;
import org.dcache.chimera.nfs.v4.xdr.fattr4_fs_layout_types;
import org.dcache.chimera.nfs.v4.xdr.fattr4_maxwrite;
import org.dcache.chimera.nfs.v4.xdr.fattr4_time_create;
import org.dcache.chimera.nfs.v4.xdr.fattr4_files_avail;
import org.dcache.chimera.nfs.v4.xdr.fattr4_mounted_on_fileid;
import org.dcache.chimera.nfs.v4.xdr.fattr4_space_total;
import org.dcache.chimera.nfs.v4.xdr.fattr4_fileid;
import org.dcache.chimera.nfs.v4.xdr.fattr4_change;
import org.dcache.chimera.nfs.v4.xdr.fattr4_symlink_support;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.fattr4_case_preserving;
import org.dcache.chimera.nfs.v4.xdr.changeid4;
import org.dcache.chimera.nfs.v4.xdr.fattr4_size;
import org.dcache.chimera.nfs.v4.xdr.fattr4_files_total;
import org.dcache.chimera.nfs.v4.xdr.fattr4_filehandle;
import org.dcache.chimera.nfs.v4.xdr.fattr4;
import org.dcache.chimera.nfs.v4.xdr.nfstime4;
import org.dcache.chimera.nfs.v4.xdr.mode4;
import org.dcache.chimera.nfs.v4.xdr.fattr4_link_support;
import org.dcache.chimera.nfs.v4.xdr.fattr4_time_modify;
import org.dcache.chimera.nfs.v4.xdr.fattr4_no_trunc;
import org.dcache.chimera.nfs.v4.xdr.fattr4_rdattr_error;
import org.dcache.chimera.nfs.v4.xdr.fattr4_files_free;
import org.dcache.chimera.nfs.v4.xdr.fattr4_time_metadata;
import org.dcache.chimera.nfs.v4.xdr.fattr4_mode;
import org.dcache.chimera.nfs.v4.xdr.fattr4_maxfilesize;
import org.dcache.chimera.nfs.v4.xdr.fattr4_acl;
import org.dcache.chimera.nfs.v4.xdr.nfsstat4;
import org.dcache.chimera.nfs.v4.xdr.fattr4_fsid;
import org.dcache.chimera.nfs.v4.xdr.fattr4_time_access;
import org.dcache.chimera.nfs.v4.xdr.fattr4_supported_attrs;
import org.dcache.chimera.nfs.v4.xdr.utf8str_mixed;
import org.dcache.chimera.nfs.v4.xdr.fattr4_space_free;
import org.dcache.chimera.nfs.v4.xdr.fattr4_cansettime;
import org.dcache.chimera.nfs.v4.xdr.fattr4_type;
import org.dcache.chimera.nfs.v4.xdr.fsid4;
import org.dcache.chimera.nfs.v4.xdr.GETATTR4resok;
import org.dcache.chimera.nfs.v4.xdr.GETATTR4res;
import org.dcache.chimera.nfs.ChimeraNFSException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.xdr.XdrAble;
import org.dcache.xdr.XdrBuffer;
import org.dcache.xdr.XdrEncodingStream;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsStat;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.nfs.v4.acl.Ace;
import org.dcache.chimera.nfs.v4.acl.AclStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationGETATTR extends AbstractNFSv4Operation {

        private static final Logger _log = LoggerFactory.getLogger(OperationGETATTR.class);

	public OperationGETATTR(nfs_argop4 args) {
		super(args, nfs_opnum4.OP_GETATTR);
	}

	@Override
	public boolean process(CompoundContext context) {

        GETATTR4res res = new GETATTR4res();

        try {

	        res.resok4 = new GETATTR4resok();
	        res.resok4.obj_attributes = getAttributes(_args.opgetattr.attr_request,
                        context.currentInode(), context);

	        res.status = nfsstat4.NFS4_OK;
        }catch(ChimeraNFSException he) {
        	res.status = he.getStatus();
        }catch(Exception e) {
            _log.error("GETATTR:", e);
            res.status = nfsstat4.NFS4ERR_RESOURCE;
        }


        _result.opgetattr = res;

            context.processedOperations().add(_result);
            return res.status == nfsstat4.NFS4_OK;

	}

    static fattr4  getAttributes(bitmap4 bitmap, FsInode inode, CompoundContext context) throws Exception {

        int[] mask = new int[bitmap.value.length];
        for( int i = 0; i < mask.length; i++) {
            mask[i] = bitmap.value[i].value;
            _log.debug("getAttributes[{}]: {}",
                    new Object[] { i, Integer.toBinaryString(mask[i])} );
        }

        int[] retMask = new int[mask.length];

        XdrEncodingStream xdr = new XdrBuffer(1024);
        xdr.beginEncoding();

        if( mask.length != 0 ) {
            int maxAttr = 32*mask.length;
            for( int i = 0; i < maxAttr; i++) {

                int newmask = (mask[i/32] >> (i-(32*(i/32))) );
                if( (newmask & 1) > 0 ) {
                        XdrAble attrXdr = fattr2xdr(i, inode, context);
                        if( attrXdr != null) {
                            _log.debug("   getAttributes : {} ({}) OK.",
                                    new Object[] { i, attrMask2String(i)} );
                            attrXdr.xdrEncode(xdr);
                            int attrmask = 1 << (i-(32*(i/32)));
                            retMask[i/32] |= attrmask;
                        }else{
                            _log.debug("   getAttributes : {} ({}) NOT SUPPORTED.",
                                    new Object[] { i, attrMask2String(i)} );
                        }
                }

            }

        }
        xdr.endEncoding();
        ByteBuffer body = xdr.body();
        byte[] retBytes = new byte[body.limit()] ;
        body.get(retBytes);

        fattr4 attributes = new fattr4();
        attributes.attrmask = new bitmap4();
        attributes.attrmask.value = new uint32_t[retMask.length];
        for( int i = 0; i < retMask.length; i++) {
            attributes.attrmask.value[i] = new uint32_t(retMask[i]);
            _log.debug("getAttributes[{}] reply : {}",
                    new Object[] { i, Integer.toBinaryString(retMask[i])} );

        }
        attributes.attr_vals = new attrlist4(retBytes);

        return attributes;

    }

    private static FsStat getFsStat(FsStat fsStat, FsInode inode) throws ChimeraFsException {
        if (fsStat != null) {
            return fsStat;
        }
        return inode.getFs().getFsStat();
    }

    /**
     * get inodes requested attribute and converted into RPC xdr format
     * operates with READ and R/W attributes
     *
     * @param fattr
     * @param inode
     * @return XdrAble of object attribute,
     * Corresponding to fattr
     * @throws Exception
     */

    // read/read-write
    private static XdrAble fattr2xdr( int fattr , FsInode inode, CompoundContext context) throws Exception {

        XdrAble ret = null;
        FsStat fsStat = null;

        switch(fattr) {

            case nfs4_prot.FATTR4_SUPPORTED_ATTRS :
                bitmap4 bitmap = new bitmap4();
                bitmap.value = new uint32_t[2];
                bitmap.value[0] = new uint32_t(NFSv4FileAttributes.NFS4_SUPPORTED_ATTRS_MASK0);
                bitmap.value[1] = new uint32_t(NFSv4FileAttributes.NFS4_SUPPORTED_ATTRS_MASK1);
                fattr4_supported_attrs  supported_attrs = new fattr4_supported_attrs(bitmap);
                ret = supported_attrs;
                break;
            case nfs4_prot.FATTR4_TYPE :
                fattr4_type type = new fattr4_type( unixType2NFS(inode.statCache().getMode()) );
                ret = type;
                break;
            case nfs4_prot.FATTR4_FH_EXPIRE_TYPE :
                uint32_t fh_type = new uint32_t(nfs4_prot.FH4_PERSISTENT);
                fattr4_fh_expire_type fh_expire_type = new fattr4_fh_expire_type(fh_type);
                ret = fh_expire_type;
                break;
            case nfs4_prot.FATTR4_CHANGE :
                changeid4 cid = new changeid4( new uint64_t(inode.stat().getMTime())  );
                fattr4_change change = new fattr4_change( cid );
                ret = change;
                break;
            case nfs4_prot.FATTR4_SIZE :
                fattr4_size size = new fattr4_size( new uint64_t(inode.statCache().getSize()) );
                ret = size;
                break;
            case nfs4_prot.FATTR4_LINK_SUPPORT :
                fattr4_link_support  link_support = new fattr4_link_support(true);
                ret = link_support;
                break;
            case nfs4_prot.FATTR4_SYMLINK_SUPPORT :
                fattr4_symlink_support  symlink_support = new fattr4_symlink_support(true);
                ret = symlink_support;
                break;
            case nfs4_prot.FATTR4_NAMED_ATTR :
                fattr4_named_attr  named_attr = new fattr4_named_attr(false);
                ret = named_attr;
                break;
            case nfs4_prot.FATTR4_FSID :
                fsid4 fsid = new fsid4();
                fsid.major = new uint64_t(17);
                fsid.minor = new uint64_t(17);
                fattr4_fsid id = new fattr4_fsid(fsid);
                ret = id;
                break;
            case nfs4_prot.FATTR4_UNIQUE_HANDLES :
                fattr4_unique_handles  unique_handles = new fattr4_unique_handles(true);
                ret = unique_handles;
                break;
            case nfs4_prot.FATTR4_LEASE_TIME :
                fattr4_lease_time lease_time = new fattr4_lease_time( new nfs_lease4(new uint32_t(NFSv4Defaults.NFS4_LEASE_TIME) ));
                ret = lease_time;
                break;
            case nfs4_prot.FATTR4_RDATTR_ERROR :
                //enum is an integer
                fattr4_rdattr_error rdattr_error = new fattr4_rdattr_error(0);
                ret = rdattr_error;
                break;
            case nfs4_prot.FATTR4_FILEHANDLE :
            	nfs_fh4 fh = new nfs_fh4();
            	fh.value = inode.toFullString().getBytes();
                fattr4_filehandle filehandle = new fattr4_filehandle(fh);
            	ret = filehandle;
                break;
            case nfs4_prot.FATTR4_ACL :

            	/*
            	 * TODO:
            	 * here is the place to talk with ACL module
            	 * for now we just reply some thing
            	 */

            	nfsace4[] aces = null;

            	/*
            	 * use dummy store
            	 */

            	nfsace4[] savedAcl = AclStore.getInstance().getAcl(inode);
            	if( savedAcl != null ) {
            		aces = new nfsace4[savedAcl.length];
            		System.arraycopy(savedAcl, 0, aces, 0, savedAcl.length);
            	}else{
            		aces = new nfsace4[0];
            	}

                _log.debug("{}",  new Ace(aces) );

            	fattr4_acl acl = new fattr4_acl(aces);

            	ret = acl;
                break;
            case nfs4_prot.FATTR4_ACLSUPPORT :
                fattr4_aclsupport aclSupport = new fattr4_aclsupport();
                aclSupport.value = new uint32_t();
                aclSupport.value.value = nfs4_prot.ACL4_SUPPORT_ALLOW_ACL |
                                         nfs4_prot.ACL4_SUPPORT_DENY_ACL;
                ret = aclSupport;
                break;
            case nfs4_prot.FATTR4_ARCHIVE :
                break;
            case nfs4_prot.FATTR4_CANSETTIME :
                fattr4_cansettime cansettime = new fattr4_cansettime(true);
                ret = cansettime;
                break;
            case nfs4_prot.FATTR4_CASE_INSENSITIVE :
                fattr4_case_insensitive caseinsensitive = new fattr4_case_insensitive(true);
                ret = caseinsensitive;
                break;
            case nfs4_prot.FATTR4_CASE_PRESERVING :
                fattr4_case_preserving casepreserving = new fattr4_case_preserving(true);
                ret = casepreserving;
                break;
            case nfs4_prot.FATTR4_CHOWN_RESTRICTED :
                break;
            case nfs4_prot.FATTR4_FILEID :
                fattr4_fileid fileid = new fattr4_fileid(  new uint64_t(inode.id()) );
                ret = fileid;
                break;
            case nfs4_prot.FATTR4_FILES_AVAIL:
                fsStat = getFsStat(fsStat, inode);
                fattr4_files_avail files_avail = new fattr4_files_avail(new uint64_t(fsStat.getTotalFiles() - fsStat.getUsedFiles()));
                ret = files_avail;
                break;
            case nfs4_prot.FATTR4_FILES_FREE:
                fsStat = getFsStat(fsStat, inode);
                fattr4_files_free files_free = new fattr4_files_free(new uint64_t(fsStat.getTotalFiles() - fsStat.getUsedFiles()));
                ret = files_free;
                break;
            case nfs4_prot.FATTR4_FILES_TOTAL:
                fsStat = getFsStat(fsStat, inode);
                fattr4_files_total files_total = new fattr4_files_total(new uint64_t(fsStat.getTotalFiles()));
                ret = files_total;
                break;
            case nfs4_prot.FATTR4_FS_LOCATIONS :
                break;
            case nfs4_prot.FATTR4_HIDDEN :
                fattr4_hidden hidden = new fattr4_hidden(false);
                ret = hidden;
                break;
            case nfs4_prot.FATTR4_HOMOGENEOUS :
                fattr4_homogeneous homogeneous = new fattr4_homogeneous(true);
                ret = homogeneous;
                break;
            case nfs4_prot.FATTR4_MAXFILESIZE :
                fattr4_maxfilesize maxfilesize = new fattr4_maxfilesize( new uint64_t(NFSv4Defaults.NFS4_MAXFILESIZE) );
                ret = maxfilesize;
                break;
            case nfs4_prot.FATTR4_MAXLINK :
                fattr4_maxlink maxlink = new fattr4_maxlink( new uint32_t(NFSv4Defaults.NFS4_MAXLINK) );
                ret = maxlink;
                break;
            case nfs4_prot.FATTR4_MAXNAME :
                fattr4_maxname maxname = new fattr4_maxname( new uint32_t(NFSv4Defaults.NFS4_MAXFILENAME) );
                ret = maxname;
                break;
            case nfs4_prot.FATTR4_MAXREAD :
                fattr4_maxread maxread = new fattr4_maxread( new uint64_t(NFSv4Defaults.NFS4_MAXIOBUFFERSIZE) );
                ret = maxread;
                break;
            case nfs4_prot.FATTR4_MAXWRITE :
                fattr4_maxwrite maxwrite = new fattr4_maxwrite( new uint64_t(NFSv4Defaults.NFS4_MAXIOBUFFERSIZE) );
                ret = maxwrite;
                break;
            case nfs4_prot.FATTR4_MIMETYPE :
                break;
            case nfs4_prot.FATTR4_MODE :
                mode4 fmode = new mode4();
                fmode.value = new uint32_t(inode.statCache().getMode() & 07777);
                fattr4_mode mode = new fattr4_mode( fmode );
                ret = mode;
                break;
            case nfs4_prot.FATTR4_NO_TRUNC :
                fattr4_no_trunc no_trunc = new    fattr4_no_trunc(true);
                ret = no_trunc;
                break;
            case nfs4_prot.FATTR4_NUMLINKS :
                uint32_t nlinks = new uint32_t(inode.statCache().getNlink());
                fattr4_numlinks numlinks = new fattr4_numlinks(nlinks);
                ret = numlinks;
                break;
            case nfs4_prot.FATTR4_OWNER :
                String owner_s = context.getIdMapping().uidToPrincipal(inode.statCache().getUid());
                utf8str_mixed user = new utf8str_mixed ( new utf8string( owner_s.getBytes()) );
                fattr4_owner owner = new fattr4_owner(user);
                ret = owner;
                break;
            case nfs4_prot.FATTR4_OWNER_GROUP :
                String group_s = context.getIdMapping().gidToPrincipal(inode.statCache().getGid());
                utf8str_mixed group = new utf8str_mixed ( new utf8string( group_s.getBytes()) );
                fattr4_owner owner_group = new fattr4_owner(group);
                ret = owner_group;
                break;
            case nfs4_prot.FATTR4_QUOTA_AVAIL_HARD :
                break;
            case nfs4_prot.FATTR4_QUOTA_AVAIL_SOFT :
                break;
            case nfs4_prot.FATTR4_QUOTA_USED :
                break;
            case nfs4_prot.FATTR4_RAWDEV :
            	specdata4 dev = new specdata4();
            	dev.specdata1 = new uint32_t(0);
            	dev.specdata2 = new uint32_t(0);
                fattr4_rawdev rawdev = new fattr4_rawdev(dev);
            	ret = rawdev;
                break;
            case nfs4_prot.FATTR4_SPACE_AVAIL:
                fsStat = getFsStat(fsStat, inode);
                uint64_t spaceAvail = new uint64_t(fsStat.getTotalSpace() - fsStat.getUsedSpace());
                ret = spaceAvail;
                break;
            case nfs4_prot.FATTR4_SPACE_FREE:
                fsStat = getFsStat(fsStat, inode);
                fattr4_space_free space_free = new fattr4_space_free(new uint64_t(fsStat.getTotalSpace() - fsStat.getUsedSpace()));
                ret = space_free;
                break;
            case nfs4_prot.FATTR4_SPACE_TOTAL:
                fsStat = getFsStat(fsStat, inode);
                fattr4_space_total space_total = new fattr4_space_total(new uint64_t(fsStat.getTotalSpace()));
                ret = space_total;
                break;
            case nfs4_prot.FATTR4_SPACE_USED :
                fattr4_space_used space_used = new fattr4_space_used ( new uint64_t(inode.statCache().getSize() ) );
                ret = space_used;
                break;
            case nfs4_prot.FATTR4_SYSTEM :
                fattr4_system system = new fattr4_system(false);
                ret = system;
                break;
            case nfs4_prot.FATTR4_TIME_ACCESS :
            	nfstime4 atime = new nfstime4();
            	atime.seconds = new int64_t(TimeUnit.SECONDS.convert(inode.statCache().getATime() , TimeUnit.MILLISECONDS));
            	atime.nseconds = new uint32_t(0);
                fattr4_time_access time_access = new fattr4_time_access(atime);
            	ret = time_access;
                break;
            case nfs4_prot.FATTR4_TIME_BACKUP :
                break;
            case nfs4_prot.FATTR4_TIME_CREATE :
            	nfstime4 ctime = new nfstime4();
            	ctime.seconds = new int64_t(TimeUnit.SECONDS.convert(inode.statCache().getCTime() , TimeUnit.MILLISECONDS));
            	ctime.nseconds = new uint32_t(0);
                fattr4_time_create time_create = new fattr4_time_create(ctime);
                ret = time_create;
                break;
            case nfs4_prot.FATTR4_TIME_DELTA :
                break;
            case nfs4_prot.FATTR4_TIME_METADATA :
                nfstime4 mdtime = new nfstime4();
                mdtime.seconds = new int64_t(TimeUnit.SECONDS.convert(inode.statCache().getCTime() , TimeUnit.MILLISECONDS));
                mdtime.nseconds = new uint32_t(0);
                fattr4_time_metadata time_metadata = new fattr4_time_metadata(mdtime);
                ret = time_metadata;
                break;
            case nfs4_prot.FATTR4_TIME_MODIFY :
            	nfstime4 mtime = new nfstime4();
            	mtime.seconds = new int64_t(TimeUnit.SECONDS.convert(inode.statCache().getMTime() , TimeUnit.MILLISECONDS));
            	mtime.nseconds = new uint32_t(0);
                fattr4_time_modify time_modify = new fattr4_time_modify(mtime);
                ret = time_modify;
                break;
            case nfs4_prot.FATTR4_MOUNTED_ON_FILEID :


                /*
                 * TODO!!!:
                 */

            	long mofi = inode.id();

                if( mofi == 0x00b0a23a /* it's a root*/ ) {
                	mofi =  0x12345678;
                }

                uint64_t rootid = new uint64_t(mofi);
                fattr4_mounted_on_fileid mounted_on_fileid = new fattr4_mounted_on_fileid(rootid);
                ret = mounted_on_fileid;
                break;

                /**
                 * this is NFSv4.1 (pNFS) specific code,
                 * which is still in the development ( as protocol )
                 */
            case nfs4_prot.FATTR4_FS_LAYOUT_TYPE:
            	fattr4_fs_layout_types fs_layout_type = new fattr4_fs_layout_types();
            	fs_layout_type.value = new int[1];
            	fs_layout_type.value[0] =  layouttype4.LAYOUT4_NFSV4_1_FILES;
            	ret = fs_layout_type;
            	break;

            case nfs4_prot.FATTR4_TIME_MODIFY_SET:
            case nfs4_prot.FATTR4_TIME_ACCESS_SET:
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_INVAL, "getattr of write-only attributes");
            default:
                _log.debug("GETATTR for #{}", fattr);

        }

        return ret;
    }


	public static String attrMask2String( int offset ) {

        String maskName = "Unknown";

        switch(offset) {

            case nfs4_prot.FATTR4_SUPPORTED_ATTRS :
                maskName=" FATTR4_SUPPORTED_ATTRS ";
                break;
            case nfs4_prot.FATTR4_TYPE :
                maskName=" FATTR4_TYPE ";
                break;
            case nfs4_prot.FATTR4_FH_EXPIRE_TYPE :
                maskName=" FATTR4_FH_EXPIRE_TYPE ";
                break;
            case nfs4_prot.FATTR4_CHANGE :
                maskName=" FATTR4_CHANGE ";
                break;
            case nfs4_prot.FATTR4_SIZE :
                maskName=" FATTR4_SIZE ";
                break;
            case nfs4_prot.FATTR4_LINK_SUPPORT :
                maskName=" FATTR4_LINK_SUPPORT ";
                break;
            case nfs4_prot.FATTR4_SYMLINK_SUPPORT :
                maskName=" FATTR4_SYMLINK_SUPPORT ";
                break;
            case nfs4_prot.FATTR4_NAMED_ATTR :
                maskName=" FATTR4_NAMED_ATTR ";
                break;
            case nfs4_prot.FATTR4_FSID :
                maskName=" FATTR4_FSID ";
                break;
            case nfs4_prot.FATTR4_UNIQUE_HANDLES :
                maskName=" FATTR4_UNIQUE_HANDLES ";
                break;
            case nfs4_prot.FATTR4_LEASE_TIME :
                maskName=" FATTR4_LEASE_TIME ";
                break;
            case nfs4_prot.FATTR4_RDATTR_ERROR :
                maskName=" FATTR4_RDATTR_ERROR ";
                break;
            case nfs4_prot.FATTR4_FILEHANDLE :
                maskName=" FATTR4_FILEHANDLE ";
                break;
            case nfs4_prot.FATTR4_ACL :
                maskName=" FATTR4_ACL ";
                break;
            case nfs4_prot.FATTR4_ACLSUPPORT :
                maskName=" FATTR4_ACLSUPPORT ";
                break;
            case nfs4_prot.FATTR4_ARCHIVE :
                maskName=" FATTR4_ARCHIVE ";
                break;
            case nfs4_prot.FATTR4_CANSETTIME :
                maskName=" FATTR4_CANSETTIME ";
                break;
            case nfs4_prot.FATTR4_CASE_INSENSITIVE :
                maskName=" FATTR4_CASE_INSENSITIVE ";
                break;
            case nfs4_prot.FATTR4_CASE_PRESERVING :
                maskName=" FATTR4_CASE_PRESERVING ";
                break;
            case nfs4_prot.FATTR4_CHOWN_RESTRICTED :
                maskName=" FATTR4_CHOWN_RESTRICTED ";
                break;
            case nfs4_prot.FATTR4_FILEID :
                maskName=" FATTR4_FILEID ";
                break;
            case nfs4_prot.FATTR4_FILES_AVAIL :
                maskName=" FATTR4_FILES_AVAIL ";
                break;
            case nfs4_prot.FATTR4_FILES_FREE :
                maskName=" FATTR4_FILES_FREE ";
                break;
            case nfs4_prot.FATTR4_FILES_TOTAL :
                maskName=" FATTR4_FILES_TOTAL ";
                break;
            case nfs4_prot.FATTR4_FS_LOCATIONS :
                maskName=" FATTR4_FS_LOCATIONS ";
                break;
            case nfs4_prot.FATTR4_HIDDEN :
                maskName=" FATTR4_HIDDEN ";
                break;
            case nfs4_prot.FATTR4_HOMOGENEOUS :
                maskName=" FATTR4_HOMOGENEOUS ";
                break;
            case nfs4_prot.FATTR4_MAXFILESIZE :
                maskName=" FATTR4_MAXFILESIZE ";
                break;
            case nfs4_prot.FATTR4_MAXLINK :
                maskName=" FATTR4_MAXLINK ";
                break;
            case nfs4_prot.FATTR4_MAXNAME :
                maskName=" FATTR4_MAXNAME ";
                break;
            case nfs4_prot.FATTR4_MAXREAD :
                maskName=" FATTR4_MAXREAD ";
                break;
            case nfs4_prot.FATTR4_MAXWRITE :
                maskName=" FATTR4_MAXWRITE ";
                break;
            case nfs4_prot.FATTR4_MIMETYPE :
                maskName=" FATTR4_MIMETYPE ";
                break;
            case nfs4_prot.FATTR4_MODE :
                maskName=" FATTR4_MODE ";
                break;
            case nfs4_prot.FATTR4_NO_TRUNC :
                maskName=" FATTR4_NO_TRUNC ";
                break;
            case nfs4_prot.FATTR4_NUMLINKS :
                maskName=" FATTR4_NUMLINKS ";
                break;
            case nfs4_prot.FATTR4_OWNER :
                maskName=" FATTR4_OWNER ";
                break;
            case nfs4_prot.FATTR4_OWNER_GROUP :
                maskName=" FATTR4_OWNER_GROUP ";
                break;
            case nfs4_prot.FATTR4_QUOTA_AVAIL_HARD :
                maskName=" FATTR4_QUOTA_AVAIL_HARD ";
                break;
            case nfs4_prot.FATTR4_QUOTA_AVAIL_SOFT :
                maskName=" FATTR4_QUOTA_AVAIL_SOFT ";
                break;
            case nfs4_prot.FATTR4_QUOTA_USED :
                maskName=" FATTR4_QUOTA_USED ";
                break;
            case nfs4_prot.FATTR4_RAWDEV :
                maskName=" FATTR4_RAWDEV ";
                break;
            case nfs4_prot.FATTR4_SPACE_AVAIL :
                maskName=" FATTR4_SPACE_AVAIL ";
                break;
            case nfs4_prot.FATTR4_SPACE_FREE :
                maskName=" FATTR4_SPACE_FREE ";
                break;
            case nfs4_prot.FATTR4_SPACE_TOTAL :
                maskName=" FATTR4_SPACE_TOTAL ";
                break;
            case nfs4_prot.FATTR4_SPACE_USED :
                maskName=" FATTR4_SPACE_USED ";
                break;
            case nfs4_prot.FATTR4_SYSTEM :
                maskName=" FATTR4_SYSTEM ";
                break;
            case nfs4_prot.FATTR4_TIME_ACCESS :
                maskName=" FATTR4_TIME_ACCESS ";
                break;
            case nfs4_prot.FATTR4_TIME_ACCESS_SET :
                maskName=" FATTR4_TIME_ACCESS_SET ";
                break;
            case nfs4_prot.FATTR4_TIME_BACKUP :
                maskName=" FATTR4_TIME_BACKUP ";
                break;
            case nfs4_prot.FATTR4_TIME_CREATE :
                maskName=" FATTR4_TIME_CREATE ";
                break;
            case nfs4_prot.FATTR4_TIME_DELTA :
                maskName=" FATTR4_TIME_DELTA ";
                break;
            case nfs4_prot.FATTR4_TIME_METADATA :
                maskName=" FATTR4_TIME_METADATA ";
                break;
            case nfs4_prot.FATTR4_TIME_MODIFY :
                maskName=" FATTR4_TIME_MODIFY ";
                break;
            case nfs4_prot.FATTR4_TIME_MODIFY_SET :
                maskName=" FATTR4_TIME_MODIFY_SET ";
                break;
            case nfs4_prot.FATTR4_MOUNTED_ON_FILEID :
                maskName=" FATTR4_MOUNTED_ON_FILEID ";
                break;
            case nfs4_prot.FATTR4_FS_LAYOUT_TYPE :
                maskName=" FATTR4_FS_LAYOUT_TYPE ";
                break;
            case nfs4_prot.FATTR4_LAYOUT_HINT:
                maskName=" FATTR4_LAYOUT_HINT ";
                break;
            case nfs4_prot.FATTR4_LAYOUT_TYPE:
                maskName=" FATTR4_LAYOUT_TYPE ";
                break;
            case nfs4_prot.FATTR4_LAYOUT_BLKSIZE:
                maskName=" FATTR4_LAYOUT_BLKSIZE ";
                break;
            case nfs4_prot.FATTR4_LAYOUT_ALIGNMENT:
                maskName=" FATTR4_LAYOUT_ALIGNMENT ";
                break;
            case nfs4_prot.FATTR4_FS_LOCATIONS_INFO:
                maskName=" FATTR4_FS_LOCATIONS_INFO ";
                break;
            case nfs4_prot.FATTR4_MDSTHRESHOLD:
                maskName=" FATTR4_MDSTHRESHOLD ";
                break;
            case nfs4_prot.FATTR4_RETENTION_GET:
                maskName=" FATTR4_RETENTION_GET ";
                break;
            case nfs4_prot.FATTR4_RETENTION_SET:
                maskName=" FATTR4_RETENTION_SET ";
                break;
            case nfs4_prot.FATTR4_RETENTEVT_GET:
                maskName=" FATTR4_RETENTEVT_GET ";
                break;
            case nfs4_prot.FATTR4_RETENTEVT_SET:
                maskName=" FATTR4_RETENTEVT_SET ";
                break;
            case nfs4_prot.FATTR4_RETENTION_HOLD:
                maskName=" FATTR4_RETENTION_HOLD ";
                break;
            case nfs4_prot.FATTR4_MODE_SET_MASKED:
                maskName=" FATTR4_MODE_SET_MASKED ";
                break;
            case nfs4_prot.FATTR4_FS_CHARSET_CAP:
                maskName=" FATTR4_FS_CHARSET_CAP ";
                break;
            default:
            	maskName += "(" + offset + ")";
        }

        return maskName;

    }


    static int unixType2NFS( int type ) {

        int ret = 0;

        int mask =  0770000;

        switch ( type & mask  ) {

            case UnixPermission.S_IFREG:
                ret = nfs_ftype4.NF4REG;
                break;
            case UnixPermission.S_IFDIR:
                ret = nfs_ftype4.NF4DIR;
                break;
            case UnixPermission.S_IFLNK:
                ret = nfs_ftype4.NF4LNK;
                break;
            case UnixPermission.S_IFSOCK:
                ret = nfs_ftype4.NF4SOCK;
                break;
            case UnixPermission.S_IFBLK:
                ret = nfs_ftype4.NF4BLK;
                break;
            case UnixPermission.S_IFCHR:
                ret = nfs_ftype4.NF4CHR;
                break;
            case UnixPermission.S_IFIFO:
                ret = nfs_ftype4.NF4FIFO;
                break;
            default:
                _log.info("Unknown mode [" + Integer.toOctalString(type) +"]");
                ret = 0;

        }

        return ret;
    }

}
