package org.dcache.acl.util.net;

import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Inet6AddressMatcher {

    private byte[] _netmask;
    private byte[] _address;

    /**
     * Creates a new address matcher.
     *
     * @param netmask
     *            of this matcher
     * @param address
     *            of this matcher
     */
    public Inet6AddressMatcher(String netmask, InetAddress address) throws UnknownHostException {
        super();
        _netmask = mask2array(netmask);
        _address = address.getAddress();
    }

//	/**
//	 * Converts a dotted IP address (a.b.c.d) to a 32-bit value.
//	 *
//	 * @param address
//	 *            The address to convert
//	 * @return The IP address as 32-bit value
//	 * @throws UnknownHostException
//	 */
//	private byte[] address2array(InetAddress address) {
//		return (new BigInteger(address.getAddress()).toByteArray());
//	}

    private byte[] mask2array(String mask) {
        byte[] bi_bytes = (new BigInteger(mask, 16)).toByteArray();
//		int bi_len = bi_bytes.length;

        byte[] bytes = new byte[16];
//		Arrays.fill(bytes, (byte)0);
//		int len = bytes.length;
        for (int i = (bytes.length-1), j = (bi_bytes.length-1); i >= 0 && j >= 0; i--, j--)
//			if ( j != 0 || bi_bytes[j] != 0)
        {
            bytes[i] = bi_bytes[j];
        }

        return bytes;
    }


    public boolean matches(InetAddress address) {
        byte[] addressBytes = address.getAddress();
        for (int index = 0; index < 16; index++) {
            if ((addressBytes[index] /*& netmask[index]*/) != (_address[index] & _netmask[index])) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param netmask
     *            The network mask to match
     * @param inetAddress
     *            The address to match
     * @return <code>true</code> if <code>address</code> matches the
     *         <code>netmask</code>, otherwise <code>false</code>
     *
     */
    public static boolean matches(String netmask, InetAddress inetAddress) throws UnknownHostException {
        Inet6AddressMatcher addressMatcher = new Inet6AddressMatcher(netmask, inetAddress);
        return addressMatcher.matches(inetAddress);
    }

    public static boolean matches(String netmask, String address) throws UnknownHostException {
        return matches(netmask, Inet6Address.getByName(address));
    }
}
