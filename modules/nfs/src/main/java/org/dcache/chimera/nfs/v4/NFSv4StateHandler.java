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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.sessionid4;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.chimera.nfs.v4.xdr.verifier4;
import org.dcache.utils.Bytes;
import org.dcache.utils.Cache;
import org.dcache.utils.Opaque;

public class NFSv4StateHandler {

    private static final Logger _log = LoggerFactory.getLogger(NFSv4StateHandler.class);

    private final List<NFS4Client> _clients = new ArrayList<>();

    // all seen by server
    private final Map<verifier4, NFS4Client> _clientsByVerifier = new HashMap<>();


    // mapping between server generated clietid and nfs_client_id, not confirmed yet
    private final Map<Long, NFS4Client> _clientsByServerId = new HashMap<>();

    private final Cache<sessionid4, NFSv41Session> _sessionById =
            new Cache<>("NFSv41 sessions", 5000, Long.MAX_VALUE, TimeUnit.SECONDS.toMillis(NFSv4Defaults.NFS4_LEASE_TIME*2));

    private final Map<Opaque, NFS4Client> _clientByOwner = new HashMap<>();

    /**
     * Client's lease expiration time in milliseconds.
     */
    private final long _leaseTime = NFSv4Defaults.NFS4_LEASE_TIME*1000;

    public void removeClient(NFS4Client client) {

        for(NFSv41Session session: client.sessions() ) {
            _sessionById.remove( session.id() );
        }

        _clientsByServerId.remove(client.getId());
        _clientByOwner.remove(client.getOwner());
        _clientsByVerifier.remove(client.verifier()) ;
        _clients.remove(client);

    }

    public void addClient(NFS4Client newClient) {
        _clients.add(newClient);
        _clientsByServerId.put(newClient.getId(), newClient);
        _clientsByVerifier.put(newClient.verifier(), newClient);
        _clientByOwner.put( newClient.getOwner(), newClient);
    }

    public NFS4Client getClientByID( Long id) throws ChimeraNFSException {
        NFS4Client client = _clientsByServerId.get(id);
        if(client == null) {
            throw new ChimeraNFSException(nfsstat.NFSERR_STALE_CLIENTID, "bad client id.");
        }
        return client;
    }

    public NFS4Client getClientIdByStateId(stateid4 stateId) throws ChimeraNFSException {
        return getClientByID(Bytes.getLong(stateId.other, 0));
    }

    public NFS4Client getClientByVerifier(verifier4 verifier) {
        return _clientsByVerifier.get(verifier);
    }

    public synchronized NFSv41Session removeSessionById(sessionid4 id) {
        return _sessionById.remove(id);
    }

    public NFSv41Session sessionById( sessionid4 id) {
       return _sessionById.get(id);
    }

    public void sessionById( sessionid4 id, NFSv41Session session) {
        _sessionById.put(id, session);
    }

    public NFS4Client clientByOwner( byte[] ownerid) {
        return _clientByOwner.get(new Opaque(ownerid));
    }

    public void updateClientLeaseTime(stateid4  stateid) throws ChimeraNFSException {

        NFS4Client client = getClientIdByStateId(stateid);
        NFS4State state = client.state(stateid);

        if( !state.isConfimed() ) {
            throw new ChimeraNFSException( nfsstat.NFSERR_BAD_STATEID, "State is not confirmed"  );
        }

        client.updateLeaseTime();
    }

    public List<NFS4Client> getClients() {
        return new CopyOnWriteArrayList<>(_clients);
    }

    public NFS4Client createClient(InetSocketAddress clientAddress, InetSocketAddress localAddress,
            byte[] ownerID, verifier4 verifier, String principal) {
        NFS4Client client = new NFS4Client(clientAddress, localAddress, ownerID, verifier, principal, _leaseTime);
        addClient(client);
        return client;
    }
}
