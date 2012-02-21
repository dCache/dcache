package diskCacheV111.poolManager;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

class NetHandler implements Serializable {
    static final long serialVersionUID = 8911153851483100573L;
    Map<Long, NetUnit>[] _netList = new HashMap[33];
    private String[] _maskStrings = new String[33];
    private long[] _masks = new long[33];

    NetHandler() {
        long mask = 0;
        long xmask = 0;
        long cursor = 1;
        for (int i = 0; i < _maskStrings.length; i++) {
            _masks[i] = xmask = ~mask;
            int a = (int) ((xmask >> 24) & 255);
            int b = (int) ((xmask >> 16) & 255);
            int c = (int) ((xmask >> 8) & 255);
            int d = (int) ((xmask) & 255);
            _maskStrings[i] = a + "." + b + "." + c + "." + d;
            mask |= cursor;
            cursor <<= 1;
        }
    }

    void clear() {
        for (int i = 0; i < _netList.length; i++) {
            if (_netList[i] != null) {
                _netList[i].clear();
            }
        }
    }

    private long inetAddressToLong(InetAddress address) {
        byte[] raw = address.getAddress();
        long addr = 0L;
        for (int i = 0; i < raw.length; i++) {
            addr <<= 8;
            addr |= raw[i] & 255;
        }
        return addr;
    }

    void add(NetUnit net) {
        int bit = net.getHostBits();
        if (_netList[bit] == null) {
            _netList[bit] = new HashMap<Long, NetUnit>();
        }
        long addr = inetAddressToLong(net.getHostAddress());
        _netList[bit].put(Long.valueOf(addr & _masks[bit]), net);
    }

    void remove(NetUnit net) {
        int bit = net.getHostBits();
        if (_netList[bit] == null) {
            return;
        }
        long addr = inetAddressToLong(net.getHostAddress());
        _netList[bit].remove(Long.valueOf(addr));
        if (_netList.length == 0) {
            _netList[bit] = null;
        }
    }

    NetUnit find(NetUnit net) {
        int bit = net.getHostBits();
        if (_netList[bit] == null) {
            return null;
        }
        long addr = inetAddressToLong(net.getHostAddress());
        return _netList[bit].get(Long.valueOf(addr & _masks[bit]));
    }

    NetUnit match(String inetAddress) throws UnknownHostException {
        long addr = inetAddressToLong(InetAddress.getByName(inetAddress));
        long mask = 0;
        long cursor = 1;
        NetUnit unit = null;
        for (Map<Long, NetUnit> map : _netList) {
            if (map != null) {
                Long l = Long.valueOf(addr & ~mask);
                unit = map.get(l);
                if (unit != null) {
                    return unit;
                }
            }
            mask |= cursor;
            cursor <<= 1;
        }
        return null;
    }

    String bitsToString(int bits) {
        return _maskStrings[bits];
    }

}
