package diskCacheV111.poolManager;

import java.net.InetAddress;
import java.util.Objects;
import org.dcache.util.Subnet;

class NetUnit extends Unit implements Comparable<NetUnit> {

    private static final long serialVersionUID = -2510355260024374990L;
    private final Subnet _subnet;

    public NetUnit(String name) {
        super(name, PoolSelectionUnit.UnitType.NET);

        _subnet = Subnet.create(name);
    }

    public InetAddress getHostAddress() {
        return _subnet.getSubnetAddress();
    }

    public int getMask() {
        return _subnet.getMask();
    }

    @Override
    public String getCanonicalName() {
        return _subnet.toString();
    }

    public boolean match(InetAddress addr) {
        // REVISIT: there are plenty of ways to mess with IPv4 and IPv6. For now, we just assume that
        // IPv4 addresses never match IPv6.
        return (addr.getClass() == _subnet.getSubnetAddress().getClass() ) && _subnet.contains(addr);
    }

    // let as sort by subnet mask: bigger mask (more precise) wins
    @Override
    public int compareTo(NetUnit o) {

        int mask = Integer.compare(o._subnet.getMask(), _subnet.getMask());
        if (mask != 0) {
            return mask;
        }

        /*
         * As TreeSet will treat two objects that compareTo return zero to be equal
         * we need to have a second comparison criteria to accept two different units
         * with the same netmask
         */

        return getName().compareTo(o.getName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NetUnit netUnit = (NetUnit) o;
        return _subnet.equals(netUnit._subnet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_subnet);
    }
}
