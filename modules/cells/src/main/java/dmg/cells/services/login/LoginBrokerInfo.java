package dmg.cells.services.login;

import com.google.common.base.Joiner;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.ProtocolFamily;
import java.net.StandardProtocolFamily;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import diskCacheV111.util.FsPath;

import org.dcache.util.NetworkUtils.InetAddressScope;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable object to capture information about a door.
 *
 * By convention, network addresses in LoginBrokerInfo should have been
 * resolved, that is, the host name should be cached within the InetAddress
 * object. This should preferably be the FQDN. The exception to this
 * rule is when the IP addresses doesn't have a corresponding name.
 */
public class LoginBrokerInfo implements Serializable
{
    private static final long serialVersionUID = 4077557054990432737L;

    private final String _cellName;
    private final String _domainName;
    private final String _protocolFamily;
    private final String _protocolVersion;
    private final String _protocolEngine;
    private final String _root;
    private final List<InetAddress> _addresses;
    @Deprecated
    private final String[] _hosts; // Kept for compatibility with pcells
    private final int _port;
    private final double _load;
    private final long _update;

    public LoginBrokerInfo(String cellName,
                           String domainName,
                           String protocolFamily,
                           String protocolVersion,
                           String protocolEngine,
                           String root,
                           List<InetAddress> addresses,
                           int port,
                           double load,
                           long updateTime)
    {
        _cellName = checkNotNull(cellName);
        _domainName = checkNotNull(domainName);
        _protocolFamily = checkNotNull(protocolFamily);
        _protocolVersion = checkNotNull(protocolVersion);
        _protocolEngine = checkNotNull(protocolEngine);
        _root = root;
        _addresses = checkNotNull(addresses);
        _port = port;
        _load = load;
        _update = updateTime;
        _hosts = new String[addresses.size()];
        for (int i = 0; i < addresses.size(); i++) {
            _hosts[i] = addresses.get(i).getHostAddress();
        }
        checkArgument(!addresses.isEmpty());
    }

    public boolean supports(InetAddressScope scope)
    {
        return _addresses.stream().anyMatch(a -> InetAddressScope.of(a).ordinal() >= scope.ordinal());
    }

    public boolean supports(ProtocolFamily family)
    {
        if (family == StandardProtocolFamily.INET) {
            return _addresses.stream().anyMatch(a -> a instanceof Inet4Address);
        } else if (family == StandardProtocolFamily.INET6) {
            return _addresses.stream().anyMatch(a -> a instanceof Inet6Address);
        }
        return true;
    }

    @Nonnull
    public List<InetAddress> getAddresses()
    {
        return Collections.unmodifiableList(_addresses);
    }

    @Deprecated
    public String[] getHosts()
    {
        return _hosts;
    }

    public int getPort()
    {
        return _port;
    }

    @Nonnull
    public String getCellName()
    {
        return _cellName;
    }

    @Nonnull
    public String getDomainName()
    {
        return _domainName;
    }

    @Nonnull
    public String getProtocolFamily()
    {
        return _protocolFamily;
    }

    @Nonnull
    public String getProtocolVersion()
    {
        return _protocolVersion;
    }

    @Nonnull
    public String getProtocolEngine()
    {
        return _protocolEngine;
    }

    public String getRoot()
    {
        return _root;
    }

    public FsPath getRoot(FsPath userRoot)
    {
        return (_root == null) ? userRoot : new FsPath(_root);
    }

    public double getLoad()
    {
        return _load;
    }

    public long getUpdateTime()
    {
        return _update;
    }

    @Nonnull
    public String getIdentifier()
    {
        return _cellName + "@" + _domainName;
    }

    public boolean equals(Object that)
    {
        if (this == that) {
            return true;
        }
        if (!(that instanceof LoginBrokerInfo)) {
            return false;
        }
        LoginBrokerInfo info = (LoginBrokerInfo) that;
        return _cellName.equals(info._cellName) && _domainName.equals(info._domainName);
    }

    public int hashCode()
    {
        return Objects.hash(_cellName, _domainName);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(_cellName).append("@").append(_domainName).append(";");
        int pos = _protocolEngine.lastIndexOf('.');
        if (pos < 0 || pos == _protocolEngine.length() - 1) {
            sb.append(_protocolEngine).append(";");
        } else {
            sb.append(_protocolEngine.substring(pos + 1)).append(";");
        }
        sb.append("{").append(_protocolFamily).append(",").
                append(_protocolVersion).append("};");

        sb.append("[");
        Joiner.on(",").appendTo(sb, _addresses);
        sb.append(":").append(_port).append("]").append(";");
        sb.append("<");
        sb.append((int) (_load * 100.)).append(",");
        sb.append(_update).append(">;");

        return sb.toString();
    }
}
