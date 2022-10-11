package diskCacheV111.poolManager;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.TreeSet;

class NetHandler implements Serializable {

    private static final long serialVersionUID = 8911153851483100573L;
    final TreeSet<NetUnit> _netListV6 = new TreeSet<>();
    final TreeSet<NetUnit> _netList = new TreeSet<>();

    void clear() {
        _netList.clear();
        _netListV6.clear();
    }

    void add(NetUnit net) {
        if (net.getHostAddress() instanceof Inet4Address) {
            _netList.add(net);
        } else {
            _netListV6.add(net);
        }
    }

    void remove(NetUnit net) {
        NetUnit n = find(net);
        _netList.remove(n);
        _netListV6.remove(n);
    }

    NetUnit find(NetUnit net) {

        /*
         * The netLists are sorted with bigger netmask first.
         * So, we walk them as long as mast big enough to fit
         * the requested subnet, and then we check the overlap.
         */
        for (var n : _netListV6) {
            if (n.getMask() < net.getMask()) {
                break;
            }
            if (n.match(net.getHostAddress())) {
                return n;
            }
        }

        for (var n : _netList) {
            if (n.getMask() < net.getMask()) {
                break;
            }
            if (n.match(net.getHostAddress())) {
                return n;
            }
        }

        return null;
    }

    NetUnit match(String inetAddress) throws UnknownHostException {
        return match(InetAddress.getByName(inetAddress));
    }

    NetUnit match(InetAddress address) {

        if (address instanceof Inet4Address) {
            for (var unit : _netList) {
                if (unit.match(address)) {
                    return unit;
                }
            }
        } else {
            for (var unit : _netListV6) {
                if (unit.match(address)) {
                    return unit;
                }
            }
        }
        return null;
    }
}
