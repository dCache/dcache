package org.dcache.pool.movers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import diskCacheV111.vehicles.ProtocolInfo;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.lang.reflect.Method;
import java.util.OptionalInt;
import javax.security.auth.Subject;
import org.dcache.auth.FQANPrincipal;
import org.junit.Before;
import org.junit.Test;

public class TransferLifeCycleTest {

    private Method getExperimentId;
    private TransferLifeCycle transferLifeCycle;

    @Before
    public void setup() throws Exception {
        transferLifeCycle = new TransferLifeCycle();
        transferLifeCycle.setVoMapping("atlas:2,cms:3");

        getExperimentId = TransferLifeCycle.class.getDeclaredMethod(
              "getExperimentId", ProtocolInfo.class, Subject.class);
        getExperimentId.setAccessible(true);
    }

    @Test
    public void shouldAcceptMinimumValidSciTagValue() throws Exception {
        OptionalInt experimentId = resolveExperimentId("64", new Subject());

        assertTrue(experimentId.isPresent());
        assertEquals(1, experimentId.getAsInt());
    }

    @Test
    public void shouldRejectSciTagValueBelowValidRange() throws Exception {
        OptionalInt experimentId = resolveExperimentId("63", new Subject());

        assertFalse(experimentId.isPresent());
    }

    @Test
    public void shouldMapSlashPrefixedFqanToVoName() throws Exception {
        Subject subject = new Subject();
        subject.getPrincipals().add(new FQANPrincipal("/atlas/usatlas", true));

        OptionalInt experimentId = resolveExperimentId("", subject);

        assertTrue(experimentId.isPresent());
        assertEquals(2, experimentId.getAsInt());
    }

    @Test
    public void shouldSuppressMarkerWhenBothEndpointsAreExcluded() throws Exception {
        assertFalse(sendsStartMarker("10.10.10.10", "10.20.20.20", "10.0.0.0/8"));
    }

    @Test
    public void shouldNotSuppressMarkerWhenOnlySourceIsExcluded() throws Exception {
        assertTrue(sendsStartMarker("10.10.10.10", "203.0.113.20", "10.0.0.0/8"));
    }

    @Test
    public void shouldNotSuppressMarkerWhenOnlyDestinationIsExcluded() throws Exception {
        assertTrue(sendsStartMarker("203.0.113.20", "10.20.20.20", "10.0.0.0/8"));
    }

    private OptionalInt resolveExperimentId(String transferTag, Subject subject) throws Exception {
        return (OptionalInt) getExperimentId.invoke(transferLifeCycle,
              new TestProtocolInfo("xrootd", transferTag), subject);
    }

    private boolean sendsStartMarker(String srcIp, String dstIp, String excludes) throws Exception {
        try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getByName("127.0.0.1"))) {
            socket.setSoTimeout(700);

            TransferLifeCycle lifecycle = new TransferLifeCycle();
            lifecycle.setEnabled(true);
            lifecycle.setVoMapping("atlas:2");
            lifecycle.setExcludes(new String[]{excludes});
            lifecycle.setFireflyDestination("127.0.0.1:" + socket.getLocalPort());

            lifecycle.onStart(
                  new InetSocketAddress(srcIp, 40000),
                  new InetSocketAddress(dstIp, 20066),
                  new TestProtocolInfo("xrootd", "129"),
                  new Subject());

            var packet = new DatagramPacket(new byte[4096], 4096);
            try {
                socket.receive(packet);
                return true;
            } catch (SocketTimeoutException ignored) {
                return false;
            }
        }
    }

    private static class TestProtocolInfo implements ProtocolInfo {

        private static final long serialVersionUID = 1L;
        private final String protocol;
        private final String transferTag;

        private TestProtocolInfo(String protocol, String transferTag) {
            this.protocol = protocol;
            this.transferTag = transferTag;
        }

        @Override
        public String getProtocol() {
            return protocol;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public int getMajorVersion() {
            return 0;
        }

        @Override
        public String getVersionString() {
            return "test";
        }

        @Override
        public String getTransferTag() {
            return transferTag;
        }
    }
}
