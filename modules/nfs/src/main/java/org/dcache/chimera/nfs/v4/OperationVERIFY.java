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
import org.dcache.chimera.nfs.v4.xdr.uint32_t;
import org.dcache.chimera.nfs.v4.xdr.fattr4;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.bitmap4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.VERIFY4res;
import org.dcache.chimera.nfs.ChimeraNFSException;
import java.util.Arrays;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationVERIFY extends AbstractNFSv4Operation {

        private static final Logger _log = LoggerFactory.getLogger(OperationVERIFY.class);

	OperationVERIFY(nfs_argop4 args) {
		super(args, nfs_opnum4.OP_VERIFY);
	}

	@Override
	public nfs_resop4 process(CompoundContext context) {

        VERIFY4res res = new VERIFY4res();

        try {

            res.status = nfsstat4.NFS4_OK;

            /*
             * Solaris client work around:
             *
             *  reply OK  in case of empty bit mask
             *
             */

            if( bitSet (_args.opverify.obj_attributes.attrmask ) ) {
                fattr4 currentAttr = OperationGETATTR.getAttributes(_args.opverify.obj_attributes.attrmask,
                        context.currentInode(), context);

                if( Arrays.equals(_args.opverify.obj_attributes.attr_vals.value, currentAttr.attr_vals.value) ) {
                    res.status = nfsstat4.NFS4_OK;
                }else{
                    res.status = nfsstat4.NFS4ERR_NOT_SAME;
                }

            }

            _log.debug("{} is same? {}", context.currentInode(), res.status);

        }catch(ChimeraNFSException he) {
        	res.status = he.getStatus();
        }catch(Exception e){
        	_log.error("VERIFY :", e);
            res.status = nfsstat4.NFS4ERR_SERVERFAULT;
        }

       _result.opverify = res;
            return _result;
	}



	private static boolean bitSet( bitmap4 bitmask) {


	    boolean set = false;

	    for( uint32_t mask: bitmask.value ) {

	        if( mask.value != 0 ) {
	            set = true;
	            break;
	        }
	    }

	    return set;
	}
}
