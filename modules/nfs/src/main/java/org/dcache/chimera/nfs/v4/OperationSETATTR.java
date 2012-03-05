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

import org.dcache.chimera.nfs.v4.xdr.int32_t;
import org.dcache.chimera.nfs.v4.xdr.utf8str_cs;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.bitmap4;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.fattr4_acl;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.settime4;
import org.dcache.chimera.nfs.v4.xdr.uint32_t;
import org.dcache.chimera.nfs.v4.xdr.fattr4;
import org.dcache.chimera.nfs.v4.xdr.time_how4;
import org.dcache.chimera.nfs.v4.xdr.nfstime4;
import org.dcache.chimera.nfs.v4.xdr.uint64_t;
import org.dcache.chimera.nfs.v4.xdr.mode4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.SETATTR4res;
import org.dcache.chimera.nfs.ChimeraNFSException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.dcache.acl.ACE;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.Who;

import org.dcache.xdr.XdrDecodingStream;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.dcache.chimera.nfs.v4.xdr.nfsace4;
import org.dcache.chimera.posix.AclHandler;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.posix.UnixAcl;
import org.dcache.xdr.XdrBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationSETATTR extends AbstractNFSv4Operation {


        private static final Logger _log = LoggerFactory.getLogger(OperationSETATTR.class);

	OperationSETATTR(nfs_argop4 args) {
		super(args, nfs_opnum4.OP_SETATTR);
	}

	@Override
	public nfs_resop4 process(CompoundContext context) {


    	SETATTR4res res = new SETATTR4res();

    	try {

    		Stat inodeStat = context.currentInode().statCache();

            UnixAcl acl = new UnixAcl(inodeStat.getUid(), inodeStat.getGid(),inodeStat.getMode() & 0777 );
            if ( ! context.getAclHandler().isAllowed(acl, context.getUser(), AclHandler.ACL_ADMINISTER) ) {
                throw new ChimeraNFSException( nfsstat.NFSERR_ACCESS, "Permission denied."  );
            }

           res.status = nfsstat.NFS_OK;
           res.attrsset = setAttributes(_args.opsetattr.obj_attributes, context.currentInode(), context);

        }catch(ChimeraNFSException hfe) {
    		res.status = hfe.getStatus();
    		res.attrsset = new bitmap4();
    		res.attrsset.value = new uint32_t[2];
    		res.attrsset.value[0] = new uint32_t(0);
    		res.attrsset.value[1] = new uint32_t(0);
    	}catch(Exception e) {
            _log.error("SETATTR4:", e);
    		res.status = nfsstat.NFSERR_SERVERFAULT;
    	}


        _result.opsetattr = res;
            return _result;

	}

    static bitmap4 setAttributes(fattr4 attributes, FsInode inode, CompoundContext context) throws Exception {

        _log.debug("set Attribute length: {}", attributes.attrmask.value.length);

        int[] mask = new int[attributes.attrmask.value.length];
        for( int i = 0; i < mask.length; i++) {
            mask[i] = attributes.attrmask.value[i].value;
            _log.debug("setAttributes[{}]: {}",
                    new Object[] {i, Integer.toBinaryString(mask[i])}
            );
        }

        XdrBuffer xdr = new XdrBuffer(attributes.attr_vals.value);
        xdr.beginDecoding();

        int[] retMask = new int[mask.length];

        if( mask.length != 0 ) {
            int maxAttr = 32*mask.length;
            for( int i = 0; i < maxAttr; i++) {
                int newmask = (mask[i/32] >> (i-(32*(i/32))) );
                if( (newmask & 1L) != 0 ) {
                    if( xdr2fattr(i, inode, context, xdr) ) {
                        _log.debug("   setAttributes : {} ({}) OK",
                            new Object[] {i, OperationGETATTR.attrMask2String(i)}
                        );
                        int attrmask = 1 << (i-(32*(i/32)));
                        retMask[i/32] |= attrmask;
                    }else{
                        _log.debug("   setAttributes : {} ({}) NOT SUPPORTED",
                            new Object[] {i, OperationGETATTR.attrMask2String(i)}
                        );
                        throw new ChimeraNFSException( nfsstat.NFSERR_ATTRNOTSUPP, "attribute "+ OperationGETATTR.attrMask2String(i) +" not supported");
                    }
                }
            }
        }

        xdr.endDecoding();


        bitmap4 bitmap = new bitmap4();
        bitmap.value = new uint32_t[retMask.length];
        for(int k = 0; k < retMask.length; k++) {
            bitmap.value[k] = new uint32_t(retMask[k]);
        }

        return bitmap;
    }

    static boolean xdr2fattr( int fattr , FsInode inode, CompoundContext context, XdrDecodingStream xdr) throws Exception {

        boolean isApplied = false;

        _log.debug("    FileAttribute: {}", fattr);

        switch(fattr) {

            case nfs4_prot.FATTR4_SIZE :

            	if( inode.isDirectory() ) {
                    throw new ChimeraNFSException(nfsstat.NFSERR_ISDIR, "path is a directory");
            	}

            	if( inode.isLink() ) {
                    throw new ChimeraNFSException(nfsstat.NFSERR_INVAL, "path is a symbolic link");
            	}

                uint64_t size = new uint64_t();
                size.xdrDecode(xdr);
                inode.setSize(size.value);
                isApplied = true;
                break;
            case nfs4_prot.FATTR4_ACL :
                fattr4_acl acl = new fattr4_acl();
                acl.xdrDecode(xdr);
                List<ACE> dacl = new ArrayList<ACE>();
                for (nfsace4 ace : acl.value) {
                    dacl.add(valueOf(ace, context.getIdMapping()));
                }
                context.getFs().setACL(inode, dacl);
                isApplied = true;
                break;
            case nfs4_prot.FATTR4_ARCHIVE :
                int32_t isArchive = new int32_t();
                isArchive.xdrDecode(xdr);
                isApplied = false;
                break;
            case nfs4_prot.FATTR4_HIDDEN :
                int32_t isHidden = new int32_t();
                isHidden.xdrDecode(xdr);
                isApplied = false;
                break;
            case nfs4_prot.FATTR4_MIMETYPE :
                utf8str_cs mimeType = new utf8str_cs();
                mimeType.xdrDecode(xdr);
                isApplied = false;
                break;
            case nfs4_prot.FATTR4_MODE :
                mode4 mode = new mode4();
                mode.xdrDecode(xdr);
                inode.setMode( mode.value.value );
                isApplied = true;
                break;
            case nfs4_prot.FATTR4_OWNER :
                // TODO: use princilat
                utf8str_cs owner = new utf8str_cs ();
                owner.xdrDecode(xdr);
                String new_owner = new String(owner.value.value);
                inode.setUID(context.getIdMapping().principalToUid(new_owner));
                isApplied = true;
                break;
            case nfs4_prot.FATTR4_OWNER_GROUP :
                // TODO: use princilat
                utf8str_cs owner_group = new utf8str_cs ();
                owner_group.xdrDecode(xdr);
                String new_group = new String(owner_group.value.value);
                inode.setGID(context.getIdMapping().principalToGid(new_group));
                isApplied = true;
                break;
            case nfs4_prot.FATTR4_SYSTEM :
                int32_t isSystem = new int32_t();
                isSystem.xdrDecode(xdr);
                isApplied = false;
                break;
            case nfs4_prot.FATTR4_TIME_ACCESS_SET :
                settime4 atime = new settime4();
                atime.xdrDecode(xdr);
                // ignore for performance
                isApplied = true;
                break;
            case nfs4_prot.FATTR4_TIME_BACKUP :
                nfstime4 btime = new nfstime4();
                btime.xdrDecode(xdr);
                isApplied = false;
                break;
            case nfs4_prot.FATTR4_TIME_CREATE :
                nfstime4 ctime = new nfstime4();
                ctime.xdrDecode(xdr);
                inode.setCTime( TimeUnit.MILLISECONDS.convert(ctime.seconds.value, TimeUnit.SECONDS) +
                		TimeUnit.MILLISECONDS.convert(ctime.nseconds.value, TimeUnit.NANOSECONDS));
                isApplied = true;
                break;
            case nfs4_prot.FATTR4_TIME_MODIFY_SET :
                settime4 setMtime = new settime4();
                setMtime.xdrDecode(xdr);

                long realMtime = 0;
                if( setMtime.set_it == time_how4.SET_TO_SERVER_TIME4 ) {
                    realMtime = System.currentTimeMillis();
                }else{
                    realMtime = TimeUnit.MILLISECONDS.convert(setMtime.time.seconds.value, TimeUnit.SECONDS) +
                    	TimeUnit.MILLISECONDS.convert(setMtime.time.nseconds.value, TimeUnit.NANOSECONDS);
                }

                inode.setMTime( realMtime );
                isApplied = true;
                break;
            case nfs4_prot.FATTR4_SUPPORTED_ATTRS:
                throw new ChimeraNFSException(nfsstat.NFSERR_INVAL, "setattr of read-only attributes");
        }

        if(!isApplied ) {
            _log.info("Attribute not applied: {}", OperationGETATTR.attrMask2String(fattr) );
        }
        return isApplied;
    }

    private static ACE valueOf(nfsace4 ace, NfsIdMapping idMapping) {

        String who = ace.who.toString();
        int type = ace.type.value.value;
        int flags = ace.flag.value.value;
        int mask = ace.access_mask.value.value;

        int id = 0;
        Who whoType = Who.valueOf(flags);
        if(whoType == Who.GROUP) {
            id = idMapping.principalToGid(who);
        } else if( whoType == Who.USER ) {
            id = idMapping.principalToUid(who);
        }

        return new ACE(AceType.valueOf(type), flags, mask, whoType, id, ACE.DEFAULT_ADDRESS_MSK);
    }
}
