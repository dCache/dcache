package org.dcache.util;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterators.*;

/**
 * Various network related utility functions.
 */
public abstract class NetworkUtils {

    private static final Logger logger = LoggerFactory.getLogger(NetworkUtils.class);

    public final static String LOCAL_HOST_ADDRESS_PROPERTY = "org.dcache.net.localaddresses";

    private final static String canonicalHostName;
    private static final int RANDOM_PORT = 23241;
    private static final int FIRST_CLIENT_HOST = 0;

    private final static List<InetAddress> LOCAL_INET_ADDRESSES;
    private final static boolean FAKED_ADDRESS;

    private final static Supplier<Iterable<InetAddress>> LOCAL_ADDRESS_SUPPLIER =
            Suppliers.memoizeWithExpiration(new LocalAddressSupplier(), 5, TimeUnit.SECONDS);

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

        canonicalHostName = getPreferredHostName();
    }

    public static String getCanonicalHostName() {
        return canonicalHostName;
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
     * Return a local address that is likely reachable from {@code expectedSource}.
     */
    public static InetAddress getLocalAddress(InetAddress expectedSource)
            throws SocketException
    {
        InetAddress localAddress = getLocalAddress(expectedSource, getProtocolFamily(expectedSource));
        if (localAddress == null) {
            if (FAKED_ADDRESS) {
                localAddress =  LOCAL_INET_ADDRESSES.get(0);
            } else {
                try (DatagramSocket socket = new DatagramSocket()) {
                    socket.connect(expectedSource, RANDOM_PORT);
                    localAddress = socket.getLocalAddress();

                    /* DatagramSocket#getLocalAddress reports errors by returning the
                     * wildcard address. There are several cases in which it does this,
                     * such as when the host it is unable to serve the protocol family,
                     * has no route to the address, or in case of Max OS X and Windows XP
                     * due to bugs (see http://goo.gl/ENXkD).
                     *
                     * We fall back to enumerating all local network addresses and choose
                     * the one with the smallest scope not smaller than the scope of the
                     * expected source.
                     */
                    if (localAddress.isAnyLocalAddress()) {
                        InetAddressScope minScope = InetAddressScope.of(expectedSource);
                        try {
                            return Ordering.natural().onResultOf(InetAddressScope.OF).min(
                                    filter(LOCAL_ADDRESS_SUPPLIER.get(),
                                           and(greaterThanOrEquals(minScope),
                                               isNotMulticast())));
                        } catch (NoSuchElementException e) {
                            localAddress = LOCAL_INET_ADDRESSES.get(0);
                        }
                    }
                }
            }
        }
        return localAddress;
    }

    /**
     * Like getLocalAddress(InetAddress), but returns an addresses from the given protocolFamily
     * that is likely reachable from {@code expectedSource}. Returns null if such an address
     * could not be determined.
     */
    public static InetAddress getLocalAddress(InetAddress expectedSource, ProtocolFamily protocolFamily)
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
            socket.connect(expectedSource, RANDOM_PORT);
            InetAddress localAddress = socket.getLocalAddress();

            /* DatagramSocket#getLocalAddress reports errors by returning the
             * wildcard address. There are several cases in which it does this,
             * such as when the host it is unable to serve the protocol family,
             * has no route to the address, or in case of Max OS X and Windows XP
             * due to bugs (see http://goo.gl/ENXkD).
             *
             * We fall back to enumerating all local network addresses and choose
             * the one with the smallest scope which matches the desired protocol
             * family and has a scope at least as big as the expected source.
             */
            if (localAddress.isAnyLocalAddress()) {
                InetAddressScope minScope = InetAddressScope.of(expectedSource);
                try {
                    return Ordering.natural().onResultOf(InetAddressScope.OF).min(
                            filter(LOCAL_ADDRESS_SUPPLIER.get(),
                                   and(greaterThanOrEquals(minScope),
                                       hasProtocolFamily(protocolFamily),
                                       isNotMulticast())));
                } catch (NoSuchElementException e) {
                    return null;
                }
            }

            /* It is quite possible that the expected source has a different protocol
             * family than the one we are expected to serve. In that case we try to
             * find a matching address from the same network interface (which we know
             * faces the expected source).
             */
            if (getProtocolFamily(localAddress) != protocolFamily) {
                InetAddressScope intendedScope = InetAddressScope.of(expectedSource);
                NetworkInterface byInetAddress = NetworkInterface.getByInetAddress(localAddress);
                try {
                    return Ordering.natural().onResultOf(InetAddressScope.OF).min(
                            Iterators.filter(forEnumeration(byInetAddress.getInetAddresses()),
                                             and(greaterThanOrEquals(intendedScope),
                                                 hasProtocolFamily(protocolFamily),
                                                 isNotMulticast())));
                } catch (NoSuchElementException e) {
                    return null;
                }
            }
            return localAddress;
        }
    }

    private static Predicate<InetAddress> isNotMulticast()
    {
        return new Predicate<InetAddress>()
        {
            @Override
            public boolean apply(InetAddress address)
            {
                return !address.isMulticastAddress();
            }
        };
    }

    private static Predicate<InetAddress> hasProtocolFamily(final ProtocolFamily protocolFamily)
    {
        return new Predicate<InetAddress>()
        {
            @Override
            public boolean apply(InetAddress address)
            {
                return getProtocolFamily(address) == protocolFamily;
            }
        };
    }

    private static Predicate<InetAddress> greaterThanOrEquals(final InetAddressScope scope)
    {
        return new Predicate<InetAddress>()
        {
            @Override
            public boolean apply(InetAddress address)
            {
                return InetAddressScope.of(address).ordinal() >= scope.ordinal();
            }
        };
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


    private static String getPreferredHostName() {
        String hostName = "localhost";

        if (!LOCAL_INET_ADDRESSES.isEmpty()) {
            InetAddress[] addresses
                = LOCAL_INET_ADDRESSES.toArray(new InetAddress[0]);
            Arrays.sort(addresses, getExternalInternalSorter());

            boolean found = false;

            /*
             * For legibility, we prefer to see a traditional
             * DNS host name; but if there is no mapping,
             * use the first address.
             */
            for (InetAddress a: addresses) {
                hostName = stripScope(a.getCanonicalHostName());

                if (!InetAddresses.isInetAddress(hostName)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                hostName = addresses[0].getCanonicalHostName();
            }
        }

        return hostName;
    }

    /*
     * Workaround for bug in Guava, which should not
     * return the scoping portion of the address.  There
     * is a patch for this, but it has not yet been
     * applied to InetAddresses in our current library version.
     */
    private static String stripScope(String hostName) {
        int i = hostName.indexOf("%");
        if (i > 0) {
            return hostName.substring(0, i);
        }
        return hostName;
    }

    /**
     * The scope of an address captures the extend of the validity of
     * an internet address.
     */
    public static enum InetAddressScope
    {
        LOOPBACK,
        LINK,
        SITE,
        GLOBAL;

        public static InetAddressScope of(InetAddress address)
        {
            if (address.isLoopbackAddress()) {
                return LOOPBACK;
            }
            if (address.isLinkLocalAddress()) {
                return LINK;
            }
            if (address.isSiteLocalAddress()) {
                return SITE;
            }
            return GLOBAL;
        }

        public static final Function<InetAddress,InetAddressScope> OF =
                new Function<InetAddress, InetAddressScope>()
                {
                    @Override
                    public InetAddressScope apply(InetAddress address)
                    {
                        return of(address);
                    }
                };
    }

    /**
     *  A supplier that returns all internet addresses of network interfaces that are up.
     */
    private static class LocalAddressSupplier implements Supplier<Iterable<InetAddress>>
    {
        @Override
        public Iterable<InetAddress> get()
        {
            try {
                return Lists.newArrayList(
                        concat(transform(forEnumeration(NetworkInterface.getNetworkInterfaces()),
                                         new Function<NetworkInterface, Iterator<InetAddress>>()
                                         {
                                             @Override
                                             public Iterator<InetAddress> apply(NetworkInterface i)
                                             {
                                                 try {
                                                     if (i.isUp()) {
                                                         return forEnumeration(i.getInetAddresses());
                                                     }
                                                 } catch (SocketException ignored) {
                                                 }
                                                 return Collections.emptyIterator();
                                             }
                                         })));
            } catch (SocketException e) {
                logger.error("Failed to resolve local network addresses: {}", e.toString());
                return Collections.emptyList();
            }
        }
    }
}
