package org.dcache.util;

import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.ImmutableList.copyOf;

import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;

/**
 * Various network related utility functions.
 */
public abstract class NetworkUtils {

    private static final int RANDOM_PORT = 23241;
    private static final int FIRST_CLIENT_HOST = 0;

    /**
     * Sorts addresses so that external addresses precede any internal interface
     * addresses.
     *
     * @return comparator for sorting array of {@link InetAddress}.
     */
    public static Comparator<InetAddress> getExternalInternalSorter() {
        return new Comparator<InetAddress>() {
            @Override
            public int compare(InetAddress arg0, InetAddress arg1) {
                if (arg0.isLinkLocalAddress()
                                || arg0.isLoopbackAddress()
                                || arg0.isSiteLocalAddress()
                                || arg0.isMulticastAddress()) {
                    return 1;
                }
                return 0;
            }
        };
    }

    /**
     * Returns the list of IP addresses of this host.
     *
     * @return
     * @throws SocketException
     */
    public static List<InetAddress> getLocalAddresses() throws SocketException {
        List<InetAddress> result = new ArrayList<InetAddress>();

        Enumeration<NetworkInterface> interfaces =
                NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface i = interfaces.nextElement();
            if (i.isUp() && !i.isLoopback()) {
                Enumeration<InetAddress> addresses = i.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    result.add(addresses.nextElement());
                }
            }
        }
        return result;
    }

    /**
     * Returns the list of IP V4 addresses of this host.
     */
    public static List<InetAddress> getLocalAddressesV4() throws SocketException {
        return copyOf(filter(getLocalAddresses(), instanceOf(Inet4Address.class)));
    }

    /**
     * Returns a local IP facing the first client address provided.
     */
    public static InetAddress getLocalAddressForClient(String[] clientHosts) throws SocketException, UnknownHostException {
        InetAddress clientAddress = InetAddress.getByName(clientHosts[FIRST_CLIENT_HOST]);
        InetAddress localAddress = NetworkUtils.getLocalAddress(clientAddress);
        return localAddress;
    }

    /**
     * Return the local address via which the given destination
     * address is reachable.
     *
     * Java does not provide this functionality and therefore we need
     * this workaround.
     */
    public static InetAddress getLocalAddress(InetAddress intendedDestination)
            throws SocketException {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(intendedDestination, RANDOM_PORT);
            return socket.getLocalAddress();
        }

    }

    /**
     * Like URI.toURL, but translates exceptions to URISyntaxException
     * with a descriptive error message.
     */
    public static URL toURL(URI uri)
        throws URISyntaxException
    {
        try {
            return uri.toURL();
        } catch (IllegalArgumentException e) {
            URISyntaxException exception =
                new URISyntaxException(uri.toString(), e.getMessage());
            exception.initCause(e);
            throw exception;
        } catch (MalformedURLException e) {
            URISyntaxException exception =
                new URISyntaxException(uri.toString(), e.getMessage());
            exception.initCause(e);
            throw exception;
        }
    }
}
