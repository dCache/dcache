package diskCacheV111.poolManager;

import java.net.Inet4Address;
import java.net.InetAddress;
import org.dcache.util.Subnet;

class NetUnit extends Unit {

    private static final long serialVersionUID = -2510355260024374990L;
    private final Subnet _subnet;

    public NetUnit(String name) {
        super(name, PoolSelectionUnit.UnitType.NET);

        _subnet = Subnet.create(name);
    }

    public int getHostBits() {
        return (_subnet.getSubnetAddress() instanceof Inet4Address ? 32 : 128) - _subnet.getMask();
    }

    public InetAddress getHostAddress() {
        return _subnet.getSubnetAddress();
    }

    @Override
    public String getCanonicalName() {
        return _subnet.toString();
    }

}
