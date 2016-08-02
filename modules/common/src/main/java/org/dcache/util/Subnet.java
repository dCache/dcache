package org.dcache.util;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

import static com.google.common.net.InetAddresses.forString;

public class Subnet implements Serializable {
    private static final long serialVersionUID = 9210530422244320383L;
    private static final Pattern MAPPED_ADDRESS = Pattern.compile("(0{1,4}:){0,12}:?:ffff(((.[0-9]{1,3}){4})|(:([0-9a-fA-F]{1,4})){2})");
    private static final Pattern DOCUMENTATION_ADDRESS = Pattern.compile("2001:0?db8:[0-9a-fA-F:]*");
    private static final Pattern TOREDO_ADDRESS = Pattern.compile("2001:0{0,4}:[0-9a-fA-F:]*");

    private static final String ALL_SUBNET = "all";

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
        if ((other == null) || !(other instanceof Subnet)) {
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
     * @param subnetAddress subnet base address
     * @param mask mask in CIDR notation (number of topmost relevant bits)
     * @return an instance of Subnet created from address and mask
     */
    public static Subnet create(InetAddress subnetAddress, int mask) {
        return new Subnet(subnetAddress, mask);
    }

    /**
     * Creates an instance of a subnet from its netmask notation.
     *
     * Examples: "123.45.0.0/24"; "1234:5678:9ABC:DEF0::/64"; 1.2.3.4/255.0.0.0
     * IPv6 compatible addresses starting with 2002: and addresses
     * of the form ::a.b.c.d are supported and will be converted to ipv4
     * addresses together with their mask (i.e., the lower 4 bytes are used
     * as mask). Mapped addresses (::ffff.a.b.c.d)
     * are converted to ipv4 addresses and should (if at all) be passed
     * with corresponding matching ipv4 mask. Teredo addresses (addresses
     * starting with 2001:0000) and special documentation addresses (2001:db8)
     * cannot be used to create instances of Subnet and will result in an
     * IllegalArgumentException.
     *
     * @param netmaskPattern CIDR notation of the subnet
     * @return an instance of Subnet created from the netmask pattern

     *
     * param netmaskPattern CIDR notation of the subnet
     * return an instance of Subnet created from the netmask pattern
     */
    public static Subnet create(String netmaskPattern) {
        String[] netmaskParts = netmaskPattern.split("/");
        String hostname = netmaskParts[0];

        InetAddress originalAddress = forString(hostname);
        boolean isIpV6Mask = originalAddress instanceof Inet6Address;

        // special handling of mapped addresses. Needed because guava's
        // forString converts these automatically to IPv4.
        if (MAPPED_ADDRESS.matcher(hostname).matches()) {
          isIpV6Mask = true;
        }

        String originalHostAddress = originalAddress.getHostAddress();

        checkArgument(!DOCUMENTATION_ADDRESS.matcher(originalHostAddress).matches(),
                "Special documentation address '%s' cannot be used to create a Subnet", hostname);

        checkArgument(!TOREDO_ADDRESS.matcher(originalHostAddress).matches(),
                "Toredo address '%s' cannot be used to create a Subnet", hostname);

        InetAddress subnetAddress = IPMatcher.tryConvertToIPv4(originalAddress);

        int maskBitLength = subnetAddress instanceof Inet4Address ? 32 : 128;

        if (netmaskParts.length > 1) {
            String maskbits = netmaskParts[1];
            int cidrMask = IPMatcher.convertToCidrIfIsIPv4Mask(maskbits);
            // if a conversion from IPv6 to IPv4 actually happened
            if (isIpV6Mask && (maskBitLength == 32)) {
                cidrMask = cidrMask==128? 32 : cidrMask & 0x1f;
            }

            checkArgument(cidrMask <= maskBitLength,
                    "Network mask '%s' in netmask pattern '%s' is too big for IP address '%s'",
                    cidrMask, netmaskPattern, originalAddress);

            return create(subnetAddress, cidrMask);

        } else {
            // if pattern only contains a hostname
            return create(subnetAddress, maskBitLength);
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
     * @param hostname hostname to be checked agains the subnet
     * @return true if hostname can be evaluated to one or more IPs that are contained in the subnet represented by this instance, otherwise false
     * @throws UnknownHostException
     */
    public boolean containsHost(String hostname) throws UnknownHostException {
        return containsAny(InetAddress.getAllByName(hostname));
    }

    /**
     * @param inetAddresses addresses to be checked against the subnet
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
     * @param inetAddress address to be checked against the subnet
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
            return _subnetAddress.getHostAddress().replaceFirst("(^|:)(0(:|$)){2,}", "::") + '/' + _mask;
        } else {
            return _subnetAddress.getHostAddress() + '/' + _mask;
        }
    }

}

