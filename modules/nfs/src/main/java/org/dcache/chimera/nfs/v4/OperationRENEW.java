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
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.RENEW4res;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationRENEW extends AbstractNFSv4Operation {


        private static final Logger _log = LoggerFactory.getLogger(OperationRENEW.class);

	OperationRENEW(nfs_argop4 args) {
		super(args, nfs_opnum4.OP_RENEW);
	}

	@Override
	public boolean process(CompoundContext context) {

        RENEW4res res = new RENEW4res();

        try {
            Long clientid = Long.valueOf(_args.oprenew.clientid.value.value);

            NFS4Client client = context.getStateHandler().getClientByID( clientid );
            if( client == null ) {
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_STALE_CLIENTID, "Bad client id");
            }

            client.updateLeaseTime(NFSv4Defaults.NFS4_LEASE_TIME);
            res.status = nfsstat4.NFS4_OK;

        }catch(ChimeraNFSException he) {
            _log.debug("RENEW: {}", he.getMessage() );
            res.status = he.getStatus();
        }


       _result.oprenew = res;

            context.processedOperations().add(_result);
            return res.status == nfsstat4.NFS4_OK;
	}

}
