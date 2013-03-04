package org.dcache.chimera.nfs.v4;

import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.chimera.nfs.v4.xdr.verifier4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NFSv4StateHandlerTest {

    private NFSv4StateHandler _stateHandler;

    @Before
    public void setUp() {
        _stateHandler = new NFSv4StateHandler();
    }

    @Test
    public void testGetByStateId() throws Exception {
        NFS4Client client = createClient(_stateHandler);

        stateid4 state = client.createState().stateid();
        _stateHandler.getClientIdByStateId(state);
    }

    @Test
    public void testGetByVerifier() throws Exception {
        NFS4Client client = createClient(_stateHandler);

        stateid4 state = client.createState().stateid();
        assertEquals(client, _stateHandler.getClientByVerifier(client.verifier()));
    }

    @Test
    public void testGetByVerifierNotExists() throws Exception {
        assertNull("get not exisintg", _stateHandler.getClientByVerifier(new verifier4()));
    }

    @Test(expected = ChimeraNFSException.class)
    public void testGetClientNotExists() throws Exception {
        _stateHandler.getClientByID(1L);
    }

    @Test
    public void testGetClientExists() throws Exception {
        NFS4Client client = createClient(_stateHandler);
        assertEquals(client, _stateHandler.getClientByID(client.getId()));
    }

    @Test
    public void testUpdateLeaseTime() throws Exception {
        NFS4Client client = createClient(_stateHandler);
        NFS4State state = client.createState();
        stateid4 stateid = state.stateid();
        state.confirm();
        _stateHandler.updateClientLeaseTime(stateid);
    }

    @Test(expected = ChimeraNFSException.class)
    public void testUpdateLeaseTimeNotConfirmed() throws Exception {
        NFS4Client client = createClient(_stateHandler);
        NFS4State state = client.createState();
        stateid4 stateid = state.stateid();

        _stateHandler.updateClientLeaseTime(stateid);
    }

    @Test(expected = ChimeraNFSException.class)
    public void testUpdateLeaseTimeNotExists() throws Exception {
        NFS4Client client = createClient(_stateHandler);
        stateid4 state = client.createState().stateid();
        _stateHandler.updateClientLeaseTime(state);
    }

    static NFS4Client createClient(NFSv4StateHandler stateHandler) throws UnknownHostException {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("www.google.com"), 123);
        return stateHandler.createClient(address, address, "123".getBytes(), new verifier4("123".getBytes()), null);
    }
}
