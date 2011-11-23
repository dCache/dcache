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
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.layoutreturn_type4;
import org.dcache.chimera.nfs.v4.xdr.layoutreturn_stateid;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.LAYOUTRETURN4res;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationLAYOUTRETURN extends AbstractNFSv4Operation {

        private static final Logger _log = LoggerFactory.getLogger(OperationLAYOUTRETURN.class);

	OperationLAYOUTRETURN(nfs_argop4 args) {
		super(args, nfs_opnum4.OP_LAYOUTRETURN);
	}

	@Override
	public nfs_resop4 process(CompoundContext context) {

	    LAYOUTRETURN4res res = new LAYOUTRETURN4res();

        _log.debug("LAYOUTRETURN4args :        type: {}", _args.oplayoutreturn.lora_layout_type);
        _log.debug("LAYOUTRETURN4args :        mode: {}", _args.oplayoutreturn.lora_iomode);
        _log.debug("LAYOUTRETURN4args : return type: {}", _args.oplayoutreturn.lora_layoutreturn.lr_returntype);
        _log.debug("LAYOUTRETURN4args :     reclaim: {}", _args.oplayoutreturn.lora_reclaim);

        try {

                if( _args.oplayoutreturn.lora_layoutreturn.lr_returntype == layoutreturn_type4.LAYOUTRETURN4_FILE) {

                context.getDeviceManager().
                        layoutReturn(context.getSession().getClient(),
                                _args.oplayoutreturn.lora_layoutreturn.lr_layout.lrf_stateid);
    		}

        	res.lorr_stateid = new layoutreturn_stateid();
        	res.lorr_stateid.lrs_present = false;
        	res.lorr_stateid.lrs_stateid = new stateid4();
        	res.lorr_stateid.lrs_stateid.seqid = new uint32_t(0);
        	res.lorr_stateid.lrs_stateid.other = new byte[12];

        	res.lorr_status = nfsstat4.NFS4_OK;

        }catch(Exception e) {
            res.lorr_status = nfsstat4.NFS4ERR_SERVERFAULT;
            _log.error("LAYOUTRETURN: ", e);
        }
      _result.oplayoutreturn = res;
            return _result;
	}

}
