package org.dcache.auth;

import com.google.common.net.InetAddresses;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
    private static final long serialVersionUID = -6791417439972410727L;

    /**
     * Request origin Internet address.
     */
    private InetAddress _address;

    /**
     * @param address
     *            Request origin Internet address.
     */
    public Origin(InetAddress address) {
        _address = address;
    }

    /**
     * @param host
     *            Request origin host name. The host name can either be a machine name, such as
     *            "java.sun.com", or a textual representation of its IP address. If the
     *            <code>host</code> is null then an InetAddress representing an address of the
     *            loopback interface is returned.
     *
     * @throws UnknownHostException
     */
    public Origin(String host) throws UnknownHostException {
        _address = InetAddress.getByName(host);
    }

    public InetAddress getAddress() {
        return _address;
    }

    public void setAddress(InetAddress address) {
        _address = address;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getName() + "]";
    }

    @Override
    public boolean equals(Object o)
    {
        if(o == this ) {
            return true;
        }

        if (!(o instanceof Origin)) {
            return false;
        }

        Origin other = (Origin) o;
        return other._address.equals(_address);
    }

    @Override
    public String getName()
    {
        return InetAddresses.toAddrString(_address);
    }

    @Override
    public int hashCode()
    {
        return _address.hashCode();
    }
}
