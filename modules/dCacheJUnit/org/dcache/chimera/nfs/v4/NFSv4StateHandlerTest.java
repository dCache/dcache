package org.dcache.chimera.nfs.v4;

import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.chimera.nfs.v4.xdr.verifier4;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class NFSv4StateHandlerTest {

    private NFSv4StateHandler _stateHandler;

    @Before
    public void setUp() {
        _stateHandler = new NFSv4StateHandler();
    }

    @Test
    public void testGetByStateId() throws Exception {
        NFS4Client client = createClient();

        stateid4 state = client.createState().stateid();
        _stateHandler.addClient(client);
        _stateHandler.getClientIdByStateId(state);
    }

    static NFS4Client createClient() throws UnknownHostException {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 123);
        NFS4Client client = new NFS4Client(address, address, "123".getBytes(),
            new verifier4("123".getBytes()), null);
        return client;
    }
}
