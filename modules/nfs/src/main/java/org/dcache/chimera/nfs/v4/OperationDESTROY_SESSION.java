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
import org.dcache.chimera.nfs.v4.xdr.DESTROY_SESSION4res;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationDESTROY_SESSION extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(OperationDESTROY_SESSION.class);

    public OperationDESTROY_SESSION(nfs_argop4 args) {
        super(args, nfs_opnum4.OP_DESTROY_SESSION);
    }

    @Override
    public nfs_resop4 process(CompoundContext context) {

        DESTROY_SESSION4res res = new DESTROY_SESSION4res();

        try {

            NFSv41Session session = context.getStateHandler().removeSessionById(_args.opdestroy_session.dsa_sessionid);
            if (session == null) {
                throw new ChimeraNFSException(nfsstat.NFSERR_BADSESSION, "client not found");
            }

            NFS4Client client = session.getClient();
            client.removeSession(session);

            /*
             * remove client if there is not sessions any more
             */
            if (client.sessions().isEmpty()) {
                _log.debug("remove client: no sessions any more");
                context.getStateHandler().removeClient(client);
            }

            res.dsr_status = nfsstat.NFS_OK;

        } catch (ChimeraNFSException hne) {
            res.dsr_status = hne.getStatus();
        } catch (Exception e) {
            _log.error("DESTROY_SESSION: ", e);
            res.dsr_status = nfsstat.NFSERR_SERVERFAULT;
        }

        _result.opdestroy_session = res;
        return _result;
    }
}
