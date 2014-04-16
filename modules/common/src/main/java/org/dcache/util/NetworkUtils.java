package org.dcache.util;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;

import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.ProtocolFamily;
import java.net.SocketException;
import java.net.StandardProtocolFamily;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;

import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.ImmutableList.copyOf;

/**
 * Various network related utility functions.
 */
public abstract class NetworkUtils {

    private static final int RANDOM_PORT = 23241;
    private static final int FIRST_CLIENT_HOST = 0;

    private final static List<InetAddress> LOCAL_INET_ADDRESSES;
    private final static boolean FAKED_ADDRESS;
    public final static String LOCAL_HOST_ADDRESS_PROPERTY = "org.dcache.net.localaddresses";

    static {
        /*
         * Get localcal Inet addresses
         */
        final String localaddresses = System.getProperty(LOCAL_HOST_ADDRESS_PROPERTY);
        final List<InetAddress> localInetAddress = new ArrayList<InetAddress>();

        if(localaddresses != null && !localaddresses.isEmpty()) {
            FAKED_ADDRESS = true;
            Splitter s = Splitter.on(',')
                    .omitEmptyStrings()
                    .trimResults();
            for(String address: s.split(localaddresses)) {
                localInetAddress.add(InetAddresses.forString(address));
            }
        } else {
            FAKED_ADDRESS = false;
            try {
                Enumeration<NetworkInterface> interfaces =
                        NetworkInterface.getNetworkInterfaces();

                while (interfaces.hasMoreElements()) {
                    NetworkInterface i = interfaces.nextElement();
                    try {
                        if (i.isUp() && !i.isLoopback()) {
                            Enumeration<InetAddress> addresses = i.getInetAddresses();
                            while (addresses.hasMoreElements()) {
                                localInetAddress.add(addresses.nextElement());
                            }
                        }
                    } catch (SocketException e) {
                        // skip faulty interface
                    }
                }
            } catch (SocketException e) {
                // huh....
            }
        }
        LOCAL_INET_ADDRESSES = ImmutableList.copyOf(localInetAddress);
    }

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
    public static List<InetAddress> getLocalAddresses() {
        return LOCAL_INET_ADDRESSES;
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
     * Like URI.toURL, but translates exceptions to URISyntaxException
     * with a descriptive error message.
     */
    public static URL toURL(URI uri)
        throws URISyntaxException
    {
        try {
            return uri.toURL();
        } catch (IllegalArgumentException | MalformedURLException e) {
            URISyntaxException exception =
                new URISyntaxException(uri.toString(), e.getMessage());
            exception.initCause(e);
            throw exception;
        }
    }

    /**
     * Return the local address via which the given destination
     * address is reachable.
     *
     * Java does not provide this functionality and therefore we need
     * this workaround.
     */
    public static InetAddress getLocalAddress(InetAddress intendedDestination)
            throws SocketException
    {
        return getLocalAddress(intendedDestination, getProtocolFamily(intendedDestination));
    }

    /**
     * Like getLocalAddress(InetAddress), but return an addresses from the given protocolFamily on
     * the network interface that would be used to reach the destination.
     */
    public static InetAddress getLocalAddress(InetAddress intendedDestination, ProtocolFamily protocolFamily)
            throws SocketException
    {
        if (FAKED_ADDRESS) {
            for (InetAddress address : LOCAL_INET_ADDRESSES) {
                if (getProtocolFamily(address) == protocolFamily) {
                    return address;
                }
            }
            return null;
        }

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(intendedDestination, RANDOM_PORT);
            InetAddress localAddress = socket.getLocalAddress();
            /* The following is a workaround for Java bugs on Mac OS X and
             * Windows XP, see eg http://goo.gl/ENXkD
             */
            if (localAddress.isAnyLocalAddress()) {
                if (intendedDestination.isLoopbackAddress()) {
                    localAddress = InetAddress.getLoopbackAddress();
                } else {
                    try {
                        localAddress = InetAddress.getLocalHost();
                    } catch (UnknownHostException e) {
                        localAddress = getLocalAddresses().get(0);
                    }
                }
            }
            if (getProtocolFamily(localAddress) != protocolFamily) {
                Enumeration<InetAddress> addresses =
                        NetworkInterface.getByInetAddress(localAddress).getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (getProtocolFamily(address) == protocolFamily && !address.isMulticastAddress() &&
                            (!address.isLoopbackAddress() || intendedDestination.isLoopbackAddress()) &&
                            (!address.isSiteLocalAddress() || intendedDestination.isSiteLocalAddress()) &&
                            (!address.isLinkLocalAddress() || intendedDestination.isLinkLocalAddress())) {
                        localAddress = address;
                        break;
                    }
                }
            }
            return localAddress;
        }
    }

    private static ProtocolFamily getProtocolFamily(InetAddress address)
    {
        if (address instanceof Inet4Address) {
            return StandardProtocolFamily.INET;
        }
        if (address instanceof Inet6Address) {
            return StandardProtocolFamily.INET6;
        }
        throw new IllegalArgumentException("Unknown protocol family: " + address);
    }
}
