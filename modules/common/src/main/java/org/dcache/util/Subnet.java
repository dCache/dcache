package org.dcache.util;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.google.common.net.InetAddresses.forString;

public class Subnet implements Serializable {
    private static final long serialVersionUID = 9210530422244320383L;

    private static final String ALL_SUBNET = "all";

    private static final int HOST_IP_INDEX   = 0;
    private static final int MASK_BITS_INDEX = 1;

    private final InetAddress _subnetAddress;
    private final int _mask;

    protected Subnet() {
        _subnetAddress = null;
        _mask = 0;
    }

    protected Subnet(InetAddress subnetAddress, int mask) {
        int hostBits = (subnetAddress instanceof Inet4Address ? 32 : 128) - mask;
        BigInteger maskedAddress = new BigInteger(subnetAddress.getAddress())
                .shiftRight(hostBits)
                .shiftLeft(hostBits);
        InetAddress address;
        try {
            address = InetAddress.getByAddress(maskedAddress.toByteArray());
        } catch (UnknownHostException uhe) {
            address = subnetAddress;
        }
        _subnetAddress = address;
        _mask = mask;
    }

    @Override
    public boolean equals(Object other) {
        if ((other == null)
                || !(other instanceof Subnet)) {
            return false;
        }

        return other.toString().equals(this.toString());
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + (this._subnetAddress != null ? this._subnetAddress.hashCode() : 0);
        hash = 53 * hash + this._mask;
        return hash;
    }

    public InetAddress getSubnetAddress() {
        return _subnetAddress;
    }

    public int getMask() {
        return _mask;
    }

    /**
     * Creates an instance of a subnet from an InetAddress and a mask in CIDR notation.
     *
     * @param subnetAddress
     * @param mask mask in CIDR notation (number of topmost relevant bits)
     * @return
     */
    public static Subnet create(InetAddress subnetAddress, int mask) {
        return new Subnet(subnetAddress, mask);
    }

    /**
     * Creates an instance of a subnet from it's CIDR notation.
     *
     * Examples: "123.45.0.0/24"; "1234:5678:9ABC:DEF0::/64"
     *
     * @param cidrPattern CIDR notation of the subnet
     * @return
     */
    public static Subnet create(String cidrPattern) {
        if (cidrPattern.equalsIgnoreCase(ALL_SUBNET)) {
            return create();
        }

        String[] net_mask = cidrPattern.split("/");
        InetAddress subnetAddress = IPMatcher.tryConvertToIPv4(forString(net_mask[HOST_IP_INDEX]));
        int maskBitLength = subnetAddress instanceof Inet4Address ? 32 : 128;

        int cidrMask;

        if (net_mask.length < 2 || (cidrMask = IPMatcher.convertIPv4MaskStringToCidr(net_mask[MASK_BITS_INDEX])) == maskBitLength) {
            return create(subnetAddress, maskBitLength);
        } else {
            return create(subnetAddress, cidrMask & (maskBitLength -1));
        }
    }

    /**
     * @return an instance of Subnet that matches all IPs
     */
    public static Subnet create() {
        return new Subnet() {
            private static final long serialVersionUID = 97750694361406752L;

            @Override
            public boolean contains(InetAddress inetAddress) {
                return true;
            }
        };
    }

    /**
     * @param hostname
     * @return true if hostname can be evaluated to one or more IPs that are contained in the subnet represented by this instance, otherwise false
     * @throws UnknownHostException
     */
    public boolean containsHost(String hostname) throws UnknownHostException {
        return containsAny(InetAddress.getAllByName(hostname));
    }

    /**
     * @param inetAddresses
     * @return true if any of the inetAddresses is contained in the subnet represented by this instance, otherwise false
     */
    public boolean containsAny(InetAddress[] inetAddresses) {
        for (InetAddress inetAddress : inetAddresses) {
            if (contains(inetAddress)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param inetAddress
     * @return true if inetAddress is contained in the subnet represented by this instance, otherwise false
     */
    public boolean contains(InetAddress inetAddress) {
        return IPMatcher.match(inetAddress, getSubnetAddress(), getMask() );
    }

    @Override
    public String toString() {
        if (_subnetAddress == null) {
            return ALL_SUBNET;
        }

        if (_subnetAddress instanceof Inet6Address) {
            return _subnetAddress.getHostAddress().replaceFirst("(^|:)(0(:|$)){2,}", "::") + "/" + _mask;
        } else {
            return _subnetAddress.getHostAddress() + "/" + _mask;
        }
    }

}

