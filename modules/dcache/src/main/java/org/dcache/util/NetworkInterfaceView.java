package org.dcache.util;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;

/**
 * This class provides an unmodifiable, serializable snapshot of a
 * {@link NetworkInterface}. It provides much the same functionality
 * as NetworkInterface; however, {@link #getParent} and {@link #getSubInterfaces}
 * return different types and the public methods will never throw a
 * {@code SocketException}.
 * <p>
 * {@link #getInterfaceAddresses} returns a list of {@link InterfaceAddressView} objects.
 */
public class NetworkInterfaceView implements Serializable {
    private static final long serialVersionUID = 5589652882519395824L;
    private static final Logger _log = LoggerFactory.getLogger(NetworkInterfaceView.class);

    private final String _displayName;
    private final byte[] _hardwareAddress;
    private final List<InterfaceAddressView> _interfaceAddresses;
    private final List<InetAddress> _inetAddresses;
    private final int _mtu;
    private final String _name;
    private final List<NetworkInterfaceView> _children;
    private final boolean _isLoopback;
    private final boolean _isPointToPoint;
    private final boolean _isUp;
    private final boolean _isVirtual;
    private final boolean _hasMulticastSupport;
    private final NetworkInterfaceView _parent;

    /**
     * Create a {@link NetworkInterfaceView} snapshot of a source
     * {@link NetworkInterface} object.  {@code source} not be a
     * sub-interface; i.e., a call to {@link NetworkInterface#getParent}
     * on {@code source} must return {@code null}.
     * <p>
     * When creating a {@code NetworkInterfaceView} object from a
     * {@code NetworkInterface} object, a {@code NetworkInterfaceView} object is created
     * for each sub-interface.  These {@code NetworkInterfaceView} objects are
     * accessible via {@link #getSubInterfaces}.
     * <p>
     * If, when creating the list of {@code NetworkInterfaceView} objects for the
     * sub-interfaces, a {@code NetworkInterfaceView} object cannot be created then
     * that sub-interface is silently skipped.
     * <p>
     * @param source the {@code NetworkInterface} from which to create a snapshot.
     * @throws SocketException if any of the {@code getHardwareAddress}, {@code getMTU},
     * {@code isLoopback}, {@code isPointToPoint}, {@code isUp} and
     * {@code supportsMulticast} methods of {@code source} throws a {@code SocketException}.
     */
    public NetworkInterfaceView(NetworkInterface source) throws SocketException {
        this(source, null);

        Preconditions.checkArgument(source.getParent() == null,
                "Cannot create NetworkInterfaceView from interface %s",
                source.getName());
    }

    private NetworkInterfaceView(NetworkInterface ni,
                                 NetworkInterfaceView parent) throws SocketException  {
        _displayName = ni.getDisplayName();
        _hardwareAddress = ni.getHardwareAddress();
        _mtu = ni.getMTU();
        _name = ni.getName();
        _isLoopback = ni.isLoopback();
        _isPointToPoint = ni.isPointToPoint();
        _isUp = ni.isUp();
        _isVirtual = ni.isVirtual();
        _hasMulticastSupport = ni.supportsMulticast();
        _parent = parent;
        _interfaceAddresses = viewOfInterfaceAddresses(ni);
        _inetAddresses = viewOfInetAddresses(ni);
        _children = viewOfChildren(ni);
    }

    private List<InterfaceAddressView> viewOfInterfaceAddresses(NetworkInterface ni) {

        ImmutableList.Builder<InterfaceAddressView> builder = ImmutableList.builder();

        for(InterfaceAddress address : ni.getInterfaceAddresses()) {
            builder.add(new InterfaceAddressView(address));
        }

        return builder.build();
    }

    private List<InetAddress> viewOfInetAddresses(NetworkInterface ni) {
        ImmutableList.Builder<InetAddress> builder = ImmutableList.builder();

        Enumeration<InetAddress> addresses = ni.getInetAddresses();

        while(addresses.hasMoreElements()) {
            InetAddress address = addresses.nextElement();
            builder.add(address);
        }

        return builder.build();
    }

    private List<NetworkInterfaceView> viewOfChildren(NetworkInterface ni) {
        ImmutableList.Builder<NetworkInterfaceView> builder = ImmutableList.builder();

        Enumeration<NetworkInterface> subInterfaces = ni.getSubInterfaces();

        while( subInterfaces.hasMoreElements()) {

            NetworkInterface child = subInterfaces.nextElement();

            try {
                NetworkInterfaceView childView = new NetworkInterfaceView(child, this);
                builder.add(childView);
            } catch (SocketException e) {
                _log.debug("Unable to add child {} of interface {}: {}", child.getName(), ni.getName(), e.getMessage());
            }
        }

        return builder.build();
    }

    public List<NetworkInterfaceView> getSubInterfaces() {
        return _children;
    }

    public Enumeration<InetAddress> getInetAddresses() {
        return Iterators.asEnumeration(_inetAddresses.iterator());
    }

    public List<InetAddress> getInetAddressList() {
        return _inetAddresses;
    }

    public boolean hasSubInterfaces() {
        return !_children.isEmpty();
    }

    public String getDisplayName() {
        return _displayName;
    }

    public byte[] getHardwareAddress() {
        return _hardwareAddress;
    }

    public List<InterfaceAddressView> getInterfaceAddresses() {
        return _interfaceAddresses;
    }

    public int getMTU() {
        return _mtu;
    }

    public String getName() {
        return _name;
    }

    public NetworkInterfaceView getParent() {
        return _parent;
    }

    public boolean isLoopback() {
        return _isLoopback;
    }

    public boolean isPointToPoint() {
        return _isPointToPoint;
    }

    public boolean isUp() {
        return _isUp;
    }

    public boolean isVirtual() {
        return _isVirtual;
    }

    public boolean supportsMulticast() {
        return _hasMulticastSupport;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("name:");

        if( getName() != null) {
            sb.append(getName());
        } else {
            sb.append("null");
        }

        if(getDisplayName() != null) {
            sb.append(" (").append(getDisplayName()).append(")");
        }


        List<InterfaceAddressView> addresses = getInterfaceAddresses();

        /*
         * NetworkInterface provides an "index" field, which isn't exposed
         * by any other method; because of this, NetworkInterfaceView
         * doesn't record the index and that part of the toString output
         * is skipped.
         */

        sb.append(" addresses:\n");

        for(InterfaceAddressView address : addresses) {
            sb.append(address.getAddress()).append(";\n");
        }

        return sb.toString();
    }

    /**
     * This class provides the same functionality as InterfaceAddress
     * but is serializable.
     */
    public static class InterfaceAddressView implements Serializable {
        private static final long serialVersionUID = 467290761384687925L;

        private final InetAddress _address;
        private final InetAddress _broadcast;
        private final short _maskLength;

        public InterfaceAddressView( InterfaceAddress ifAddress) {
            _address = ifAddress.getAddress();
            _broadcast = ifAddress.getBroadcast();
            _maskLength = ifAddress.getNetworkPrefixLength();
        }

        public InetAddress getAddress() {
            return _address;
        }

        public InetAddress getBroadcast() {
            return _broadcast;
        }

        public short getNetworkPrefixLength() {
            return _maskLength;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getNetworkPrefixLength(), getAddress(), getBroadcast());
        }

        @Override
        public boolean equals(Object rawOther) {


            if( rawOther == this) {
                return true;
            }

            if (rawOther == null){
                return false;
            }

            if(rawOther.getClass() == this.getClass()) {
                InterfaceAddressView other = (InterfaceAddressView) rawOther;

                return _maskLength == other.getNetworkPrefixLength() &&
                    Objects.equal(_address, other.getAddress()) &&
                    Objects.equal(_broadcast, other.getBroadcast());
            }

            return false;
        }

        @Override
        public String toString() {
            return _address.toString() + "/" + _maskLength + " [" + _broadcast + "]";
        }
    }
}
