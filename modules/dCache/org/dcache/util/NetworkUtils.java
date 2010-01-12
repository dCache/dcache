package org.dcache.util;

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
public abstract class NetworkUtils
{
    /**
     * Returns the list of IP V4 addresses of this host.
     */
    public static List<InetAddress> getLocalAddressesV4()
        throws SocketException
    {
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
}