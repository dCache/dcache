package org.dcache.util;

import java.net.DatagramSocket;
import java.util.List;
import java.util.ArrayList;
import java.util.Enumeration;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.SocketException;

/**
 * Various network related utility functions.
 */
public abstract class NetworkUtils {

    private static final int RANDOM_PORT = 23241;

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
}
