package org.dcache.acl;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.dcache.acl.enums.AuthType;
import java.security.Principal;

/**
 * An object of type Origin contains information about the origin of a
 * request.
 *
 * Principals such as this Origin may be associated with a particular
 * Subject to augment that Subject with an additional identity. Refer
 * to the Subject class for more information on how to achieve
 * this. Authorization decisions can then be based upon the Principals
 * associated with a Subject.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class Origin implements Principal, Serializable
{
    static final long serialVersionUID = -6791417439972410727L;

    private static final char SEPARATOR = ':';

    /**
     * Type of authentication.
     */
    private AuthType _authType;

    // /**
    // * Internet address type (e.g. ipv4, ipv6).
    // */
    // private InetAddressType addressType;

    /**
     * Request origin Internet address.
     */
    private InetAddress _address;

    /**
     * @param authType
     *            Type of authentication.
     *
     * @param address
     *            Request origin Internet address.
     */
    public Origin(AuthType authType, InetAddress address) {
        _authType = authType;
        _address = address;
    }

    /**
     * @param authType
     *            Type of authentication.
     * @param host
     *            Request origin host name. The host name can either be a machine name, such as
     *            "java.sun.com", or a textual representation of its IP address. If the
     *            <code>host</code> is null then an InetAddress representing an address of the
     *            loopback interface is returned.
     *
     * @throws UnknownHostException
     */
    public Origin(AuthType authType, String host) throws UnknownHostException {
        _authType = authType;
        _address = InetAddress.getByName(host);
    }

    public InetAddress getAddress() {
        return _address;
    }

    public void setAddress(InetAddress address) {
        _address = address;
    }

    public AuthType getAuthType() {
        return _authType;
    }

    public void setAuthType(AuthType authType) {
        _authType = authType;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        if ( _authType != null )
            sb.append(_authType.getAbbreviation());
        sb.append(SEPARATOR).append(_address);
        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (!(o instanceof Origin)) {
            return false;
        }

        Origin other = (Origin) o;
        return
            other._authType == _authType &&
            other._address.equals(_address);
    }

    public String getName()
    {
        return toString();
    }

    public int hashCode()
    {
        return _address.hashCode();
    }
}
