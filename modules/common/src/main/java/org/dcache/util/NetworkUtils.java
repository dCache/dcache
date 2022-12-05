package org.dcache.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.and;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.filter;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various network related utility functions.
 */
public abstract class NetworkUtils {

    /**
     * Accept a space-separated list of identifiers that are then used to build the supplied list of
     * InetAddress.  Each identifier may be an IP address or a hostname.  Host names are resolved
     * when an object is created and that lookup is subsequently cached.  If any identifier is a
     * wildcard IP address (0.0.0.0 or ::) or hosts is null then the supplied addresses will include
     * all local addresses.
     */
    private static class HostListAddressSupplier implements
          java.util.function.Supplier<List<InetAddress>> {

        private final List<InetAddress> fixedAddresses;
        private final java.util.function.Supplier<List<InetAddress>> dynamicAddresses;

        public HostListAddressSupplier(@Nullable String hosts) throws UnknownHostException {
            boolean wildcard = false;
            ImmutableList.Builder<InetAddress> builder = ImmutableList.builder();
            for (String host : Splitter.on(' ').omitEmptyStrings().split(hosts)) {
                if (isInetAddress(host)) {
                    InetAddress address = InetAddresses.forString(host);
                    checkArgument(!address.isMulticastAddress(),
                          "Invalid address %s: cannot publish a multicast address", host);
                    if (address.isAnyLocalAddress()) {
                        wildcard = true;
                    } else {
                        builder.add(withCanonicalAddress(address));
                    }
                } else {
                    builder.add(InetAddress.getByName(host)); // REVISIT InetAddress#getAllByName ?
                }
            }
            dynamicAddresses = wildcard ? new AnyAddressSupplier() : () -> Collections.emptyList();
            fixedAddresses = builder.build();
        }

        @Override
        public List<InetAddress> get() {
            List<InetAddress> dynamic = dynamicAddresses.get();
            if (dynamic.isEmpty()) {
                return fixedAddresses;
            } else if (fixedAddresses.isEmpty()) {
                return dynamic;
            } else {
                List<InetAddress> combined = new ArrayList<>();
                combined.addAll(dynamic);
                combined.addAll(fixedAddresses);
                return combined;
            }
        }
    }

    /**
     * A supplier that returns all Internet addresses of network interfaces that are both up and not
     * a loopback interface.
     */
    public static class AnyAddressSupplier extends LocalAddressSupplier {

        private List<InetAddress> _previous = Collections.emptyList();

        @Override
        public List<InetAddress> get() {
            NDC.push("NIC auto-discovery");
            try {
                Stopwatch stopwatch = Stopwatch.createStarted();
                List<InetAddress> addresses = super.get();
                logger.debug("Scan took {}", stopwatch);
                logChanges(addresses);
                return addresses;
            } finally {
                NDC.pop();
            }
        }

        private synchronized void logChanges(List<InetAddress> addresses) {
            if (!_previous.equals(addresses)) {
                List<InetAddress> added = addresses.stream().filter(a -> !_previous.contains(a))
                      .collect(toList());
                List<InetAddress> removed = _previous.stream().filter(a -> !addresses.contains(a))
                      .collect(toList());

                boolean adding = !added.isEmpty();
                boolean removing = !removed.isEmpty();

                if (removing || adding) {
                    StringBuilder sb = new StringBuilder();
                    if (removing) {
                        sb.append("Removing ").append(describeList(removed));
                    }

                    if (adding) {
                        if (removing) {
                            sb.append(", adding ");
                        } else {
                            sb.append("Adding ");
                        }
                        sb.append(describeList(added));
                    }
                    logger.warn(sb.toString());
                }

                _previous = new ArrayList<>(addresses);
            }
        }

        private static String describeList(List<InetAddress> addresses) {
            if (addresses.size() == 1) {
                return addresses.get(0).toString();
            } else {
                return addresses.stream().map(NetworkUtils::toString)
                      .collect(joining(", ", "[", "]"));
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(NetworkUtils.class);

    public static final String LOCAL_HOST_ADDRESS_PROPERTY = "org.dcache.net.localaddresses";

    private static String canonicalHostName;
    private static final int RANDOM_PORT = 23241;

    private static final List<InetAddress> FAKED_ADDRESSES;

    private static final Supplier<List<InetAddress>> LOCAL_ADDRESS_SUPPLIER =
          Suppliers.memoizeWithExpiration(new LocalAddressSupplier(), 5, TimeUnit.SECONDS);

    static {
        String value = nullToEmpty(System.getProperty(LOCAL_HOST_ADDRESS_PROPERTY));
        ImmutableList.Builder<InetAddress> fakedAddresses = ImmutableList.builder();
        for (String address : Splitter.on(',').omitEmptyStrings().trimResults().split(value)) {
            try {
                fakedAddresses.add(InetAddress.getByName(address));
            } catch (UnknownHostException e) {
                throw new RuntimeException("Can't resolve fake hostname " + address + " provided by " + LOCAL_HOST_ADDRESS_PROPERTY, e);
            }
        }
        FAKED_ADDRESSES = fakedAddresses.build();
    }

    public static synchronized String getCanonicalHostName() {
        if (canonicalHostName == null) {
            canonicalHostName = getPreferredHostName();
        }
        return canonicalHostName;
    }

    /**
     * Returns the list of IP addresses of this host.
     *
     * @return
     * @throws SocketException
     */
    public static Collection<InetAddress> getLocalAddresses() {
        if (!FAKED_ADDRESSES.isEmpty()) {
            return FAKED_ADDRESSES;
        }
        return Collections2.filter(LOCAL_ADDRESS_SUPPLIER.get(), isNotMulticast());
    }

    /**
     * Like URI.toURL, but translates exceptions to URISyntaxException with a descriptive error
     * message.
     */
    public static URL toURL(URI uri)
          throws URISyntaxException {
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
          throws SocketException {
        InetAddress localAddress = getLocalAddress(expectedSource,
              getProtocolFamily(expectedSource));
        if (localAddress == null) {
            if (!FAKED_ADDRESSES.isEmpty()) {
                localAddress = FAKED_ADDRESSES.get(0);
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
                            return Ordering.natural().onResultOf(InetAddressScope::of).min(
                                  filter(LOCAL_ADDRESS_SUPPLIER.get(),
                                        and(greaterThanOrEquals(minScope),
                                              isNotMulticast())));
                        } catch (NoSuchElementException e) {
                            throw new SocketException(
                                  "Unable to find address that faces " + expectedSource);
                        }
                    }
                }
            }
        }
        return localAddress;
    }

    /**
     * Like getLocalAddress(InetAddress), but returns an addresses from the given protocolFamily
     * that is likely reachable from {@code expectedSource}. Returns null if such an address could
     * not be determined.
     */
    public static InetAddress getLocalAddress(InetAddress expectedSource,
          ProtocolFamily protocolFamily)
          throws SocketException {
        if (!FAKED_ADDRESSES.isEmpty()) {
            for (InetAddress address : FAKED_ADDRESSES) {
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
                    return Ordering.natural().onResultOf(InetAddressScope::of).min(
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
                    return Ordering.natural().onResultOf(InetAddressScope::of).min(
                          Iterators.filter(byInetAddress.getInetAddresses().asIterator(),
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

    public static InetAddress getInternalAddress(String ipString)
          throws IllegalArgumentException, UnknownHostException {
        if (!Strings.isNullOrEmpty(ipString)) {
            InetAddress address = InetAddresses.forString(ipString);
            if (address.isAnyLocalAddress()) {
                throw new IllegalArgumentException(
                      "Wildcard address is not a valid local address: " + address);
            }
            return address;
        } else {
            return InetAddress.getLocalHost();
        }
    }

    private static Predicate<InetAddress> isNotMulticast() {
        return address -> !address.isMulticastAddress();
    }

    private static Predicate<InetAddress> hasProtocolFamily(final ProtocolFamily protocolFamily) {
        return address -> getProtocolFamily(address) == protocolFamily;
    }

    private static Predicate<InetAddress> greaterThanOrEquals(final InetAddressScope scope) {
        return address -> InetAddressScope.of(address).ordinal() >= scope.ordinal();
    }

    public static ProtocolFamily getProtocolFamily(InetAddress address) {
        if (address instanceof Inet4Address) {
            return StandardProtocolFamily.INET;
        }
        if (address instanceof Inet6Address) {
            return StandardProtocolFamily.INET6;
        }
        throw new IllegalArgumentException("Unknown protocol family: " + address);
    }

    public static String toString(InetAddress a) {
        String name = a.getHostName();
        if (InetAddresses.isInetAddress(name)) {
            return InetAddresses.toAddrString(a);
        } else {
            return name + "/" + InetAddresses.toUriString(a);
        }
    }


    private static String getPreferredHostName() {
        List<InetAddress> addresses =
              Ordering.natural().onResultOf(InetAddressScope::of).reverse()
                    .sortedCopy(getLocalAddresses());
        if (addresses.isEmpty()) {
            return "localhost";
        }
        /* For legibility, we prefer to see a traditional
         * DNS host name; but if there is no mapping,
         * use the first address.
         */
        for (InetAddress a : addresses) {
            String hostName = stripScope(a.getCanonicalHostName());
            if (!InetAddresses.isInetAddress(hostName)) {
                return hostName;
            }
        }
        return addresses.get(0).getCanonicalHostName();
    }

    /*
     * Workaround for bug in Guava, which should not
     * return the scoping portion of the address.  There
     * is a patch for this, but it has not yet been
     * applied to InetAddresses in our current library version.
     */
    private static String stripScope(String hostName) {
        int i = hostName.indexOf('%');
        if (i > 0) {
            return hostName.substring(0, i);
        }
        return hostName;
    }

    public static boolean isInetAddress(String hostname) {
        return InetAddresses.isInetAddress(stripScope(hostname));
    }

    /**
     * Returns an InetAddress with the result of InetAddress#getCanonicalHostName filled in as the
     * hostname. Subsequent calls to InetAddress#getHostName will return the canonical name without
     * further lookups.
     */
    public static InetAddress withCanonicalAddress(InetAddress address) {
        try {
            String name = address.getCanonicalHostName();

            // Java uses an extension to IPv6 addressing
            // [draft-ietf-ipngwg-scoping-arch-04.txt] where a '%' is appended
            // to the String representation of an IPv6 link-local and
            // site-local address to disambiguate addresses that are potentially
            // not globally unique.
            //
            // For dCache, this makes no sense: the zone identifiers are local
            // to the door (e.g., "eth0", "eth1", etc).  There is no guarantee
            // the client machine will share the same mapping; e.g., the link-
            // local address #1 accessible via eth0 on the door may be accessible
            // via eth1 on the client machine.
            //
            // Therefore we strip off any zone identifiers, if no canonical name
            // is provided.  This makes a tacit assumption that any site-local
            // or link-local address is unique to clients that can connect over
            // those addresses.
            //
            // Note that, due to a bug in Guava[1], we can't detect when the
            // canonical is an IP address; however, as '%' is not a character
            // for a DNS entry, we can apply the work-around for all IPv6
            // addresses.
            //
            // [1] https://code.google.com/p/guava-libraries/issues/detail?id=1557
            //
            if (address instanceof Inet6Address) {
                name = stripScope(name);
            }

            return InetAddress.getByAddress(name, address.getAddress());
        } catch (UnknownHostException e) {
            return address;
        }
    }

    /**
     * The scope of an address captures the extend of the validity of an internet address.
     */
    public enum InetAddressScope {
        LOOPBACK,
        LINK,
        SITE,
        GLOBAL;

        public static InetAddressScope of(InetAddress address) {
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

    }

    public static java.util.function.Supplier<List<InetAddress>> anyAddressSupplier() {
        return new AnyAddressSupplier();
    }

    /**
     * Accept a space-separated list of identifiers that are then used to build the supplied list of
     * InetAddress.  Each identifier may be an IP address or a hostname.  Hostnames are resolved
     * when an object is created and that lookup is subsequently cached.  If any identifier is a
     * wildcard IP address (0.0.0.0 or ::) then the supplied addresses will include all local
     * addresses.
     */
    public static java.util.function.Supplier<List<InetAddress>> hostListAddressSupplier(
          String list)
          throws UnknownHostException {
        return new HostListAddressSupplier(list);
    }

    private static boolean isUp(NetworkInterface i) {
        try {
            return i.isUp();
        } catch (SocketException e) {
            return false;
        }
    }

    /**
     * if address hols a reference to the interface, then this reverence will be preserved
     * during serialization/deserialization, thus have a performance impact.
     */
    private static InetAddress interfaceFreeCopy(InetAddress src) {

        var bytes = src.getAddress();
        try {
            return InetAddress.getByAddress(bytes);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to create new instance of InetAddress", e);
        }
    }

    /**
     * A supplier that returns all Internet addresses of network interfaces that are up.  REVISIT:
     * LocalAddressSupplier and AnyAddressSupplier are essentially the same and should be merged.
     */
    private static class LocalAddressSupplier implements Supplier<List<InetAddress>> {

        @Override
        public List<InetAddress> get() {
            try {
                /*
                 * Get IP addresses from all interfaces. As InetAddress objects returned by
                 * NetworkInterface contain interface names, deserialization of them will
                 * trigger interface re-discovery. Re-create InetAddress objects with address
                 * information only.
                 */
                return NetworkInterface.networkInterfaces()
                      .filter(NetworkUtils::isUp)
                      .flatMap(NetworkInterface::inetAddresses)
                      .filter(not(InetAddress::isLinkLocalAddress))
                      .filter(not(InetAddress::isLoopbackAddress))
                      .map(NetworkUtils::interfaceFreeCopy)
                      .collect(toList());

            } catch (SocketException e) {
                logger.error("Failed to resolve local network addresses: {}", e.toString());
                return Collections.emptyList();
            }
        }
    }
}
