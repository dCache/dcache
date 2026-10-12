package dmg.cells.network;

import static org.junit.Assert.assertEquals;

import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellDomainRole;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import javax.net.SocketFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocationManagerConnectorTest {

    private static final String REMOTE_DOMAIN = "remoteDomain";

    private ServerSocket _remote;
    private LocationManagerConnector _connector;
    private TestCellDomain _domain;

    @Before
    public void setUp() throws Exception {
        _domain = TestCellDomain.LOCAL;
        _remote = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        _remote.setSoTimeout(10_000);
    }

    @After
    public void tearDown() throws Exception {
        if (_connector != null) {
            _connector.getNucleus().kill(_connector.getCellName());
        }
        _remote.close();
    }

    @Test
    public void shouldAnnounceCoreRoleToRemoteDomain() throws Exception {
        assertEquals(CellDomainRole.CORE, announcedDomainInfo(CellDomainRole.CORE).getRole());
    }

    @Test
    public void shouldAnnounceSatelliteRoleToRemoteDomain() throws Exception {
        assertEquals(CellDomainRole.SATELLITE,
              announcedDomainInfo(CellDomainRole.SATELLITE).getRole());
    }

    @Test
    public void shouldAnnounceLocalDomainNameToRemoteDomain() throws Exception {
        assertEquals(_domain.getCellDomainName(),
              announcedDomainInfo(CellDomainRole.CORE).getCellDomainName());
    }

    /**
     * Starts a connector with the given role and returns the domain info the tunnel presents to
     * the remote domain during the handshake.
     */
    private CellDomainInfo announcedDomainInfo(CellDomainRole role) throws Exception {
        _connector = new LocationManagerConnector("c-" + REMOTE_DOMAIN + '*',
              SocketFactory.getDefault(), REMOTE_DOMAIN,
              new InetSocketAddress(_remote.getInetAddress(), _remote.getLocalPort()), role);
        _connector.start().get();

        try (Socket socket = _remote.accept()) {
            socket.setSoTimeout(10_000);
            return (CellDomainInfo) new ObjectInputStream(socket.getInputStream()).readObject();
        }
    }
}
