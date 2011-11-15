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

import java.util.List;
import org.dcache.chimera.nfs.v4.xdr.nfsstat4;
import org.dcache.chimera.nfs.v4.xdr.sessionid4;
import org.dcache.chimera.nfs.v4.xdr.uint32_t;
import org.dcache.chimera.nfs.v4.xdr.slotid4;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.SEQUENCE4res;
import org.dcache.chimera.nfs.v4.xdr.SEQUENCE4resok;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OperationSEQUENCE extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(OperationSEQUENCE.class);

    public OperationSEQUENCE(nfs_argop4 args) {
        super(args, nfs_opnum4.OP_SEQUENCE);
    }

    @Override
    public boolean process(CompoundContext context) {
       SEQUENCE4res res = new SEQUENCE4res();
       boolean cached = false;

        try {
            /*
             *
             * from NFSv4.1 spec:
             *
             * This operation MUST appear as the first operation of any COMPOUND in which it appears.
             * The error NFS4ERR_SEQUENCE_POS will be returned when if it is found in any position in
             * a COMPOUND beyond the first. Operations other than SEQUENCE, BIND_CONN_TO_SESSION,
             * EXCHANGE_ID, CREATE_SESSION, and DESTROY_SESSION, may not appear as the first operation
             * in a COMPOUND. Such operations will get the error NFS4ERR_OP_NOT_IN_SESSION if they do
             * appear at the start of a COMPOUND.
             *
             */


            if(context.processedOperations().size() != 0 ) {
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_SEQUENCE_POS, "SEQUENCE not a first operation");
            }

            res.sr_resok4 = new SEQUENCE4resok();

            res.sr_resok4.sr_highest_slotid = new slotid4(_args.opsequence.sa_highest_slotid.value);
            res.sr_resok4.sr_slotid = new slotid4(_args.opsequence.sa_slotid.value);
            res.sr_resok4.sr_target_highest_slotid = new slotid4(_args.opsequence.sa_slotid.value);
            res.sr_resok4.sr_sessionid = new sessionid4(_args.opsequence.sa_sessionid.value);

            NFSv41Session session = context.getStateHandler().sessionById(_args.opsequence.sa_sessionid);

            if(session == null ) {
                _log.debug("no session for id [{}]",  _args.opsequence.sa_sessionid );
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_BADSESSION, "client not found");
            }

            NFS4Client client = session.getClient();

            if( client.sessionsEmpty(session)) {
                _log.debug("no client for session for id [{}]",  _args.opsequence.sa_sessionid );
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_BADSESSION, "client not found");
            }


            List<nfs_resop4> reply = context.processedOperations();
            cached = session.updateSlot(_args.opsequence.sa_slotid.value.value,
                    _args.opsequence.sa_sequenceid.value.value,
                    _args.opsequence.sa_cachethis, reply);

            session.getClient().updateLeaseTime(NFSv4Defaults.NFS4_LEASE_TIME);

            context.setSession(session);

            //res.sr_resok4.sr_sequenceid = new sequenceid4( new uint32_t( session.nextSequenceID()) );
            res.sr_resok4.sr_sequenceid = _args.opsequence.sa_sequenceid;
            res.sr_resok4.sr_status_flags = new uint32_t(0);


            res.sr_status = nfsstat4.NFS4_OK;
        }catch(ChimeraNFSException ne) {
            _log.debug("SEQUENCE : {}", ne.getMessage());
            res.sr_status = ne.getStatus();
        }catch(Exception e) {
            _log.error("SEQUENCE :", e);
            res.sr_status = nfsstat4.NFS4ERR_SERVERFAULT;
        }

        _result.opsequence = res;
        if (cached) {
            context.processedOperations().set(0, _result);
            return false;
        }

        context.processedOperations().add(_result);
        return res.sr_status == nfsstat4.NFS4_OK;
    }

}
