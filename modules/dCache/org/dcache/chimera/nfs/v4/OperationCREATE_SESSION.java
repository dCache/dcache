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
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.CREATE_SESSION4resok;
import org.dcache.chimera.nfs.v4.xdr.CREATE_SESSION4res;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationCREATE_SESSION extends AbstractNFSv4Operation {


        private static final Logger _log = LoggerFactory.getLogger(OperationCREATE_SESSION.class);

	public OperationCREATE_SESSION(nfs_argop4 args) {
		super(args, nfs_opnum4.OP_CREATE_SESSION);
	}

	@Override
	public boolean process(CompoundContext context) {
    	CREATE_SESSION4res res = new CREATE_SESSION4res();

    	Long clientId = Long.valueOf(_args.opcreate_session.csa_clientid.value.value);

    	try {

    		/*
    		 * check for correct arguments
    		 */
    		if( _args.opcreate_session.csa_fore_chan_attrs.ca_rdma_ird.length > 1 ) {
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_BADXDR, "bad size of rdma_ird");
    		}

    		/*
    		 * check for correct flags
    		 */
    		if( ( _args.opcreate_session.csa_flags.value
    				& ~( nfs4_prot.CREATE_SESSION4_FLAG_PERSIST |nfs4_prot.CREATE_SESSION4_FLAG_CONN_RDMA |
    				nfs4_prot.CREATE_SESSION4_FLAG_CONN_BACK_CHAN)) != 0) {

                throw new ChimeraNFSException(nfsstat4.NFS4ERR_INVAL, "bad ceate_session flag");
    		}

    		NFS4Client client = context.getStateHandler().getClientByID(clientId);

    		/*
    		 * Phase 1:
    		 *
             * Client record lookup. The server looks up the client ID in its client record table.
             * If the server contains no records with client ID equal to clientid_arg,
             * then most likely the client's state has been purged during a period of inactivity,
             * possibly due to a loss of connectivity. NFS4ERR_STALE_CLIENTID is returned,
             * and no changes are made to any client records on the server.
             * Otherwise, the server goes to phase 2.
             */
    		if(client == null ) {
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_STALE_CLIENTID, "client not known");
    		}

    		/*
    		 * Phase 2:
    		 *
    		 * Sequence id processing. If csa_sequenceid is equal to the sequence id in the client ID's slot,
    		 * then this is a replay of the previous CREATE_SESSION request, and the server returns
    		 * the cached result. If csa_sequenceid is not equal to the sequence id in the slot,
    		 * and is more than one greater (accounting for wraparound),
    		 * then the server returns the error NFS4ERR_SEQ_MISORDERED, and does not change the slot.
    		 * If csa_sequenceid is equal to the slot's sequence id + 1 (accounting for wraparound),
    		 * then the slot's sequence id is set to csa_sequenceid, and the CREATE_SESSION processing
    		 * goes to the next phase. A subsequent new CREATE_SESSION call MUST use a csa_sequence
    		 * that is one greater than last successfully used.
    		 */

    		if( !client.principal().equals(Integer.toString(context.getUser().getUID())) && !client.isConfirmed() ) {
                throw new ChimeraNFSException(nfsstat4.NFS4ERR_CLID_INUSE, "client already in use: " + client.principal()+ " " + context.getUser().getUID());
    		}

        	NFSv41Session session = client.createSession(_args.opcreate_session.csa_sequence.value.value,
                        _args.opcreate_session.csa_fore_chan_attrs.ca_maxrequests.value.value);
                _log.debug("adding new session [{}]", session);
                context.getStateHandler().sessionById(session.id(), session);

    		client.refreshLeaseTime();
    		//client.updateLeaseTime(NFSv4Defaults.NFS4_LEASE_TIME);

	    	res.csr_resok4 = new CREATE_SESSION4resok();

                res.csr_resok4.csr_sessionid = session.id();
	    	res.csr_resok4.csr_sequence = _args.opcreate_session.csa_sequence;

	    	/* we do not support callback connections on the same line*/
	    	res.csr_resok4.csr_flags = new uint32_t( _args.opcreate_session.csa_flags.value ^ nfs4_prot.CREATE_SESSION4_FLAG_CONN_BACK_CHAN);
	    	/*res.csr_resok4.csr_headerpadsize = _args.opcreate_session.csa_headerpadsize;

	    	res.csr_resok4.csr_conn_binding_opts = new conn_binding4res();
	    	res.csr_resok4.csr_conn_binding_opts.cbr_enforce = false;
	    	res.csr_resok4.csr_conn_binding_opts.cbr_hash_alg_info = new hash_alg_info4();*/

	    	res.csr_resok4.csr_fore_chan_attrs = _args.opcreate_session.csa_fore_chan_attrs;
	    	res.csr_resok4.csr_back_chan_attrs = _args.opcreate_session.csa_back_chan_attrs;

	    	res.csr_status = nfsstat4.NFS4_OK;

        }catch(ChimeraNFSException ne) {
            _log.debug("CREATE_SESSION4res : {}",  ne.getMessage());
    		res.csr_status = ne.getStatus();
    	}catch(Exception e) {
            _log.error("CREATE_SESSION4 :", e);
    		res.csr_status = nfsstat4.NFS4ERR_SERVERFAULT;
    	}

       _result.opcreate_session = res;

            context.processedOperations().add(_result);
            return res.csr_status == nfsstat4.NFS4_OK;
	}

}
