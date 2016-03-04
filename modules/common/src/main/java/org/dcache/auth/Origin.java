package org.dcache.auth;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.net.InetAddresses;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
    private final InetAddress _address;

    /**
     * The client chain: contains _address as the first item.  If null,
     * equivalent to a single item.
     */
    private final ImmutableList<InetAddress> _clientChain;

    /**
     * @param address
     *            Request origin Internet address.
     */
    public Origin(InetAddress address) {
        _address = checkNotNull(address);
        _clientChain = ImmutableList.of(address);
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
        _address = InetAddress.getByName(checkNotNull(host));
        _clientChain = ImmutableList.of(_address);
    }

    /**
     * The list of client addresses.  The first address is that of the client
     * making direct connection with dCache.  The n+1 address is that of the
     * client making direct connection to the service with the n address.
     */
    public Origin(ImmutableList<InetAddress> addresses) {
        checkArgument(!addresses.isEmpty(), "Empty client chain");
        _address = addresses.get(0);
        _clientChain = addresses;
    }

    public InetAddress getAddress() {
        return _address;
    }

    public ImmutableList<InetAddress> getClientChain() {
        return _clientChain == null ? ImmutableList.of(_address) : _clientChain;
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
        return getClientChain().equals(other.getClientChain());
    }

    @Override
    public String getName()
    {
        if (_clientChain == null || _clientChain.size() == 1) {
            return InetAddresses.toAddrString(_address);
        }

        StringBuilder sb = new StringBuilder();
        UnmodifiableIterator<InetAddress> i = _clientChain.iterator();
        for (;;) {
            sb.append(InetAddresses.toAddrString(i.next()));
            if (!i.hasNext()) {
                return sb.toString();
            }
            sb.append(',');
        }
    }

    @Override
    public int hashCode()
    {
        return _address.hashCode();
    }
}
