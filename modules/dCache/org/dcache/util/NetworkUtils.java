package org.dcache.util;

import java.net.DatagramSocket;
import java.util.List;
import java.util.ArrayList;
import java.util.Enumeration;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.URL;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.MalformedURLException;

import org.apache.log4j.Logger;

/**
 * Various network related utility functions.
 */
public abstract class NetworkUtils {

    private static final int RANDOM_PORT = 23241;
    private static final int FIRST_CLIENT_HOST = 0;
    private static final Logger _log = Logger.getLogger(NetworkUtils.class);

    /**
     * Returns the list of IP V4 addresses of this host.
     */
    public static List<InetAddress> getLocalAddressesV4()
            throws SocketException {
        List<InetAddress> result = new ArrayList<InetAddress>();

        Enumeration<NetworkInterface> interfaces =
                NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface i = interfaces.nextElement();
            if (i.isUp() && !i.isLoopback()) {
                Enumeration<InetAddress> addresses = i.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address) {
                        result.add(address);
                    }
                }
            }
        }
        return result;
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
        DatagramSocket socket = new DatagramSocket();
        try {
            socket.connect(intendedDestination, RANDOM_PORT);
            return socket.getLocalAddress();
        } finally {
            socket.close();
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
