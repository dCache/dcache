package dmg.cells.network;

import static org.junit.Assert.assertEquals;

import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellDomainRole;
import dmg.util.DummyStreamEngine;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.dcache.util.Args;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocationMgrTunnelTest {

    private ServerSocket _remote;
    private Socket _local;
    private TestCellDomain _domain;

    @Before
    public void setUp() throws Exception {
        _domain = TestCellDomain.LOCAL;
        _remote = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        _remote.setSoTimeout(10_000);
        _local = new Socket(_remote.getInetAddress(), _remote.getLocalPort());
    }

    @After
    public void tearDown() throws Exception {
        _local.close();
        _remote.close();
    }

    @Test
    public void shouldAnnounceCoreRolePassedAsArgument() throws Exception {
        assertEquals(CellDomainRole.CORE, announcedDomainInfo(new Args("-role=core")).getRole());
    }

    @Test
    public void shouldAnnounceSatelliteRolePassedAsArgument() throws Exception {
        assertEquals(CellDomainRole.SATELLITE,
              announcedDomainInfo(new Args("-role=satellite")).getRole());
    }

    @Test
    public void shouldIgnoreCaseOfRolePassedAsArgument() throws Exception {
        assertEquals(CellDomainRole.CORE, announcedDomainInfo(new Args("-role=CoRe")).getRole());
    }

    @Test
    public void shouldAnnounceRoleWhenOtherArgumentsArePresent() throws Exception {
        assertEquals(CellDomainRole.CORE,
              announcedDomainInfo(new Args("-prot=raw -role=core -lm=lm")).getRole());
    }

    @Test
    public void shouldAnnounceSatelliteRoleIfNoRoleArgument() throws Exception {
        assertEquals(CellDomainRole.SATELLITE,
              announcedDomainInfo(new Args("-prot=raw")).getRole());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectUnknownRolePassedAsArgument() throws Exception {
        new LocationMgrTunnel("tunnel*", new DummyStreamEngine(_local), new Args("-role=bogus"));
    }

    @Test
    public void shouldAnnounceRolePassedExplicitly() throws Exception {
        LocationMgrTunnel tunnel = new LocationMgrTunnel("tunnel*", new DummyStreamEngine(_local),
              CellDomainRole.CORE);
        assertEquals(CellDomainRole.CORE, announcedDomainInfo(tunnel).getRole());
    }

    private CellDomainInfo announcedDomainInfo(Args args) throws Exception {
        return announcedDomainInfo(
              new LocationMgrTunnel("tunnel*", new DummyStreamEngine(_local), args));
    }

    /**
     * Starts the tunnel and returns the domain info it presents to the remote domain during the
     * handshake.
     */
    private CellDomainInfo announcedDomainInfo(LocationMgrTunnel tunnel) throws Exception {
        tunnel.start();
        try (Socket socket = _remote.accept()) {
            socket.setSoTimeout(10_000);
            return (CellDomainInfo) new ObjectInputStream(socket.getInputStream()).readObject();
        }
    }
}
