package org.dcache.util;

import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;

/**
 * Set of tests for the NetworkInterfaceView class
 * <p>
 * We are somewhat limited in what we can test as NetworkInterface
 * is a final class that doesn't implement any interface and there
 * are no static methods for creating NetworkInterface objects other
 * than those for returning information about what is available.
 * <p>
 * These tests will use all the interfaces of the test-machine from
 * which we can create a NetworkInterfaceView snapshot as the fixture.
 * Any interface that we cannot create a snapshot of is silently ignored.
 * See {@link NetworkInterfaceView} for reasons why creating a snapshot
 * might fail.
 * <p>
 * These tests are bypassed (they are not run but without reporting a
 * failure) if we can't enumerate the available interfaces or if we
 * can't take a snapshot of at least one interface.
 */
public class NetworkInterfaceViewTests {

    List<InterfaceAndViewPair> _interfaces;

    @Before
    public void setUp()
    {
        ImmutableList.Builder<InterfaceAndViewPair> builder = ImmutableList.builder();

        Enumeration<NetworkInterface> interfaces;
        try {
            interfaces = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            fail("Could not enumerate interfaces");
            throw new RuntimeException("Code cannot reach this point"); // work-around for Java
        }

        while(interfaces.hasMoreElements()) {
            NetworkInterface ni = interfaces.nextElement();

            try {
                NetworkInterfaceView niv = new NetworkInterfaceView(ni);
                builder.add(new InterfaceAndViewPair(ni, niv));
            } catch( SocketException e) {
                // Silently skip interfaces where no snapshot can be created.
            }
        }

        _interfaces = builder.build();
        assertTrue(_interfaces.size() > 0);
    }

    @Test
    public void testGetDisplayName()  {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterface ni = pair.getNetworkInterface();
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();

            assertEquals(ni.getDisplayName(), niv.getDisplayName());
        }
    }

    @Test
    public void testGetHardwareAddress() throws SocketException {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterface ni = pair.getNetworkInterface();
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();

            assertArrayEquals(ni.getHardwareAddress(), niv.getHardwareAddress());
        }
    }

    @Test
    public void testGetInetAddresses() {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterface ni = pair.getNetworkInterface();
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();

            List<InetAddress> niInetAddresses = Collections.list(ni.getInetAddresses());
            List<InetAddress> nivInetAddresses = Collections.list(niv.getInetAddresses());

            assertEquals(niInetAddresses, nivInetAddresses);
        }
    }

    @Test
    public void testGetInterfaceAddresses() {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterface ni = pair.getNetworkInterface();
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();

            List<InterfaceAddress> sourceAddresses = ni.getInterfaceAddresses();
            List<NetworkInterfaceView.InterfaceAddressView> viewAddresses = niv.getInterfaceAddresses();

            assertEquals(sourceAddresses.size(), viewAddresses.size());

            for( int i = 0; i < sourceAddresses.size(); i++) {
                InterfaceAddress source = sourceAddresses.get(i);
                NetworkInterfaceView.InterfaceAddressView expected = new NetworkInterfaceView.InterfaceAddressView(source);
                NetworkInterfaceView.InterfaceAddressView actual = viewAddresses.get(i);

                assertEquals(expected, actual);
                assertEquals(expected.toString(), actual.toString());
            }
        }
    }

    @Test
    public void testGetMTU() throws SocketException {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterface ni = pair.getNetworkInterface();
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();

            assertEquals(ni.getMTU(), niv.getMTU());
        }
    }

    @Test
    public void testGetName() {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterface ni = pair.getNetworkInterface();
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();

            assertEquals(ni.getName(), niv.getName());
        }
    }

    @Test
    public void testGetParent() {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();

            assertNull(niv.getParent());
            assertAllSubInterfacesHaveParent(niv);
        }
    }

    @Test
    public void testIsLoopback() throws SocketException {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterface ni = pair.getNetworkInterface();
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();
            assertEquals(ni.isLoopback(), niv.isLoopback());
        }
    }

    @Test
    public void testIsPointToPoint() throws SocketException {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterface ni = pair.getNetworkInterface();
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();
            assertEquals(ni.isPointToPoint(), niv.isPointToPoint());
        }
    }

    @Test
    public void testIsUp() throws SocketException {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterface ni = pair.getNetworkInterface();
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();
            assertEquals(ni.isUp(), niv.isUp());
        }
    }

    @Test
    public void testIsVirtual() {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterface ni = pair.getNetworkInterface();
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();
            assertEquals(ni.isVirtual(), niv.isVirtual());
        }
    }

    @Test
    public void testSupportsMulticast() throws SocketException {
        for(InterfaceAndViewPair pair : _interfaces) {
            NetworkInterface ni = pair.getNetworkInterface();
            NetworkInterfaceView niv = pair.getNetworkInterfaceView();
            assertEquals(ni.supportsMulticast(), niv.supportsMulticast());
        }
    }

    private void assertAllSubInterfacesHaveParent(NetworkInterfaceView parent) {
        for( NetworkInterfaceView subInterface : parent.getSubInterfaces()) {
            assertEquals(parent, subInterface.getParent());
            assertAllSubInterfacesHaveParent(subInterface);
        }
    }

    /**
     * A simple data class to hold a network interface and
     * the corresponding NetworkInterfaceView snapshot.
     */
    private static class InterfaceAndViewPair {
        private final NetworkInterface _ni;
        private final NetworkInterfaceView _niv;
        public InterfaceAndViewPair(NetworkInterface ni, NetworkInterfaceView niv) {
            _ni = ni;
            _niv = niv;
        }

        public NetworkInterface getNetworkInterface() {
            return _ni;
        }

        public NetworkInterfaceView getNetworkInterfaceView() {
            return _niv;
        }
    }
}
