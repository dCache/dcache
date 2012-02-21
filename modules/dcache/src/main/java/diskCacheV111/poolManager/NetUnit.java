package diskCacheV111.poolManager;

import java.net.InetAddress;
import java.net.UnknownHostException;

class NetUnit extends Unit {
    static final long serialVersionUID = -2510355260024374990L;
    private InetAddress _address = null;
    private long _mask = 0;
    private int _bits = 0;
    private String _canonicalName = null;
    private NetHandler _netHandler = null;

    public NetUnit(String name, NetHandler netHandler) throws UnknownHostException {
        super(name, PoolSelectionUnitV2.NET);
        _netHandler = netHandler;

        int n = name.indexOf('/');
        if (n < 0) {
            //
            // no netmask found (is -host)
            //
            _address = InetAddress.getByName(name);
            //
        } else {
            if ((n == 0) || (n == (name.length() - 1))) {
                throw new IllegalArgumentException("host or net part missing");
            }
            String hostPart = name.substring(0, n);
            String netPart = name.substring(n + 1);
            //
            // count hostbits
            //
            byte[] raw = InetAddress.getByName(netPart).getAddress();
            _mask = ((raw[0] & 255) << 24) | ((raw[1] & 255) << 16) | ((raw[2] & 255) << 8) | (raw[3] & 255);
            long cursor = 1;
            _bits = 0;
            for (_bits = 0; _bits < 32; _bits++) {
                if ((_mask & cursor) > 0) {
                    break;
                }
                cursor <<= 1;
            }
            _address = InetAddress.getByName(hostPart);
        }
        _canonicalName = _address.getHostAddress() + "/" + _netHandler.bitsToString(_bits);
    }

    public int getHostBits() {
        return _bits;
    }

    public InetAddress getHostAddress() {
        return _address;
    }

    @Override
    public String getCanonicalName() {
        return _canonicalName;
    }

}
