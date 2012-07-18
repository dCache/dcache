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

import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.SETCLIENTID_CONFIRM4res;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationSETCLIENTID_CONFIRM extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(OperationPUTFH.class);

    OperationSETCLIENTID_CONFIRM(nfs_argop4 args) {
        super(args, nfs_opnum4.OP_SETCLIENTID_CONFIRM);
    }

    @Override
    public nfs_resop4 process(CompoundContext context) {

        SETCLIENTID_CONFIRM4res res = new SETCLIENTID_CONFIRM4res();

        try {
            Long clientid = _args.opsetclientid_confirm.clientid.value.value;

            NFS4Client client = context.getStateHandler().getClientByID(clientid);
            if (client == null) {
                throw new ChimeraNFSException(nfsstat.NFSERR_STALE_CLIENTID, "Bad client id");
            }

            res.status = nfsstat.NFSERR_INVAL;
            if ( client.verifierEquals(_args.opsetclientid_confirm.setclientid_confirm)) {
                res.status = nfsstat.NFS_OK;
                client.setConfirmed();
            }
        } catch (ChimeraNFSException he) {
            _log.debug("SETCLIENTID_CONFIRM: {}", he.getMessage());
            res.status = he.getStatus();
        }

        _result.opsetclientid_confirm = res;
        return _result;
    }
}
