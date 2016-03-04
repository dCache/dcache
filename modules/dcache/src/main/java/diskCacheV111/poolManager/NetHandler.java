package diskCacheV111.poolManager;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

class NetHandler implements Serializable {
    private static final long serialVersionUID = 8911153851483100573L;
    final Map<Long, NetUnit>[] _netList = new HashMap[33];
    private final String[] _maskStrings = new String[33];
    private final long[] _masks = new long[33];
    final Map<BigInteger, NetUnit>[] _netListV6 = new HashMap[129];
    private final String[] _maskStringsV6 = new String[129];
    private final BigInteger[] _masksV6 = new BigInteger[129];

    NetHandler() {
        long mask = 0;
        long xmask;
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

        BigInteger maskV6 = BigInteger.ZERO;
        BigInteger xmaskV6;
        BigInteger cursorV6 = BigInteger.ONE;
        for (int i = 0; i < _maskStringsV6.length; i++) {
            xmaskV6 = maskV6.not();
            _masksV6[i] = xmaskV6;
            _maskStringsV6[i] = Integer.toString(xmaskV6.bitCount());
            maskV6 = maskV6.or(cursorV6);
            cursorV6 = cursorV6.shiftLeft(1);
        }
    }

    void clear() {
        for (Map<Long, NetUnit> netMap : _netList) {
            if (netMap != null) {
                netMap.clear();
            }
        }
        for (Map<BigInteger, NetUnit> netMap : _netListV6) {
            if (netMap != null) {
                netMap.clear();
            }
        }
    }

    private BigInteger inetAddressToBigInteger(InetAddress address) {
        return new BigInteger(address.getAddress());
    }

    private long inetAddressToLong(InetAddress address) {
        long value = 0;
        int i=24;
        for(byte b : address.getAddress()) {
            long bv = b >= 0 ? (long)b : (long)256+b;
            value |= bv << i;
            i -= 8;
        }
        return value;
    }

    void add(NetUnit net) {
        int bit = net.getHostBits();
        if (net.getHostAddress() instanceof Inet4Address) {
            if (_netList[bit] == null) {
                _netList[bit] = new HashMap<>();
            }
            long addr = inetAddressToLong(net.getHostAddress());
            long maskedAddr = addr & _masks[bit];
            _netList[bit].put(maskedAddr, net);
        } else {
            if (_netListV6[bit] == null) {
                _netListV6[bit] = new HashMap<>();
            }
            BigInteger addr = inetAddressToBigInteger(net.getHostAddress());
            _netListV6[bit].put(addr.and(_masksV6[bit]), net);
        }
    }

    void remove(NetUnit net) {
        int bit = net.getHostBits();
        if (net.getHostAddress() instanceof Inet4Address) {
            if (_netList[bit] == null) {
                return;
            }
            long addr = inetAddressToLong(net.getHostAddress());
            _netList[bit].remove(addr & _masks[bit]);
            if (_netList.length == 0) {
                _netList[bit] = null;
            }
        } else {
            if (_netListV6[bit] == null) {
                return;
            }
            BigInteger addr = inetAddressToBigInteger(net.getHostAddress());
            _netListV6[bit].remove(addr.and(_masksV6[bit]));
            if (_netList.length == 0) {
                _netListV6[bit] = null;
            }
        }
    }

    NetUnit find(NetUnit net) {
        int bit = net.getHostBits();
        NetUnit result = null;
        if (_netListV6[bit] != null) {
           BigInteger addr = inetAddressToBigInteger(net.getHostAddress());
           result = _netListV6[bit].get(addr.and(_masksV6[bit]));
        }
        if (result == null && _netList[bit] != null) {
            long addr = inetAddressToLong(net.getHostAddress());
            result = _netList[bit].get(addr & _masks[bit]);
        }
        return result;
    }

    NetUnit match(String inetAddress) throws UnknownHostException {
        InetAddress address = InetAddress.getByName(inetAddress);
        if (address instanceof Inet4Address) {
            long addr = inetAddressToLong(address);
            long mask = 0;
            long cursor = 1;
            NetUnit unit;
            for (Map<Long, NetUnit> map : _netList) {
                if (map != null) {
                    Long l = addr & ~mask;
                    unit = map.get(l);
                    if (unit != null) {
                        return unit;
                    }
                }
                mask |= cursor;
                cursor <<= 1;
            }
        } else {
           BigInteger addr = inetAddressToBigInteger(address);
           BigInteger mask = BigInteger.ZERO;
           BigInteger cursor = BigInteger.ONE;
           NetUnit unit;
           for (Map<BigInteger, NetUnit> map : _netListV6) {
               if (map != null) {
                   BigInteger l = addr.and(mask.not());
                   unit = map.get(l);
                   if (unit != null) {
                       return unit;
                   }
               }
               mask = mask.or(cursor);
               cursor = cursor.shiftLeft(1);
           }
        }
        return null;
    }

    String bitsToString(int bits) {
        return _maskStringsV6[bits];
    }

}
