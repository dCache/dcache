package dmg.cells.services.login;

import com.google.common.base.Joiner;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.ProtocolFamily;
import java.net.StandardProtocolFamily;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import diskCacheV111.util.FsPath;

import org.dcache.util.NetworkUtils.InetAddressScope;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;
import static java.util.Objects.requireNonNull;

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

    public enum Capability
    {
        READ, WRITE
    }

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
    private final Collection<String> _tags;
    private final Collection<String> _readPaths;
    private final Collection<String> _writePaths;

    private transient FsPath _rootFsPath;
    private transient Collection<FsPath> _readFsPaths;
    private transient Collection<FsPath> _writeFsPaths;

    public LoginBrokerInfo(String cellName,
                           String domainName,
                           String protocolFamily,
                           String protocolVersion,
                           String protocolEngine,
                           String root,
                           Collection<String> readPaths,
                           Collection<String> writePaths,
                           Collection<String> tags,
                           List<InetAddress> addresses,
                           int port,
                           double load,
                           long updateTime)
    {
        checkArgument(!addresses.isEmpty());
        _cellName = requireNonNull(cellName);
        _domainName = requireNonNull(domainName);
        _protocolFamily = requireNonNull(protocolFamily);
        _protocolVersion = requireNonNull(protocolVersion);
        _protocolEngine = requireNonNull(protocolEngine);
        _root = root;
        _tags = requireNonNull(tags);
        _readPaths = requireNonNull(readPaths);
        _writePaths = requireNonNull(writePaths);
        _addresses = requireNonNull(addresses);
        _port = port;
        _load = load;
        _update = updateTime;
        _hosts = new String[addresses.size()];
        for (int i = 0; i < addresses.size(); i++) {
            _hosts[i] = addresses.get(i).getHostAddress();
        }
        if (_root != null) {
            _rootFsPath = FsPath.create(_root);
        }
        _readFsPaths = _readPaths.stream().map(FsPath::create).collect(toList());
        _writeFsPaths = _writePaths.stream().map(FsPath::create).collect(toList());
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
        return (_rootFsPath == null) ? userRoot : _rootFsPath;
    }

    public String relativize(FsPath userRoot, FsPath path)
    {
        return path.stripPrefix(getRoot(userRoot));
    }

    public boolean canWrite(FsPath userRoot, FsPath path)
    {
        return path.hasPrefix(getRoot(userRoot)) &&
               _writeFsPaths.stream().anyMatch(path::hasPrefix);
    }

    public boolean canRead(FsPath userRoot, FsPath path)
    {
        return path.hasPrefix(getRoot(userRoot)) &&
               _readFsPaths.stream().anyMatch(path::hasPrefix);
    }

    public Collection<String> getTags()
    {
        return Collections.unmodifiableCollection(_tags);
    }

    public Collection<String> getReadPaths()
    {
        return Collections.unmodifiableCollection(_readPaths);
    }

    public Collection<String> getWritePaths()
    {
        return Collections.unmodifiableCollection(_writePaths);
    }

    public void ifCapableOf(Capability capability, Consumer<LoginBrokerInfo> action)
    {
        Collection<String> paths;
        switch (capability) {
        case READ:
            paths = _readPaths;
            break;
        case WRITE:
            paths = _writePaths;
            break;
        default:
            paths = Collections.emptyList();
            break;
        }
        if (!paths.isEmpty()) {
            action.accept(this);
        }
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
        return _cellName + '@' + _domainName;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(_cellName).append('@').append(_domainName).append(';');
        int pos = _protocolEngine.lastIndexOf('.');
        if (pos < 0 || pos == _protocolEngine.length() - 1) {
            sb.append(_protocolEngine).append(';');
        } else {
            sb.append(_protocolEngine.substring(pos + 1)).append(';');
        }
        sb.append('{').append(_protocolFamily).append(',').
                append(_protocolVersion).append("};");

        sb.append('[');
        Joiner.on(",").appendTo(sb, _addresses);
        sb.append(':').append(_port).append(']').append(';');
        sb.append('<');
        sb.append((int) (_load * 100.)).append(',');
        sb.append(_update).append(">;");

        return sb.toString();
    }

    private void readObject(ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        if (_root != null) {
            _rootFsPath = FsPath.create(_root);
        }
        _readFsPaths = _readPaths.stream().map(FsPath::create).collect(toList());
        _writeFsPaths = _writePaths.stream().map(FsPath::create).collect(toList());
    }
}
