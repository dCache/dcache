// $Id$

package diskCacheV111.util;

import java.io.Serializable;

/**
 * Immutable representation of a pnfsId
 */
public class PnfsId implements Serializable, Comparable<PnfsId> {

    private final static int OLD_ID_SIZE = 12; // original pnfs
    private final static int NEW_ID_SIZE = 18; // chimera

    private final byte[] _a;
    private final String _idString;
    private final String _domain;

    // we call to often toString() on pnfsId
    private final String _toString;

    private static final long serialVersionUID = -112220393521303857L;

    public PnfsId(String idString, String domain) {
        this(idString + "." + domain);
    }

    public PnfsId(String idString) {
        int i = idString.indexOf('.');
        if (i < 0) {
            _a = _stringToBytes(idString);
            _domain = null;
        } else {
            _a = _stringToBytes(idString.substring(0, i));
            if (i < (idString.length() - 1)) {
                _domain = idString.substring(i + 1);
            } else {
                _domain = null;
            }
        }
        _idString = bytesToHexString(_a);
        _toString = _idString + (_domain != null ? "." + _domain : "");
    }

    public PnfsId(byte[] idBytes) {

        int length = idBytes.length;
        if (length != OLD_ID_SIZE && length != NEW_ID_SIZE) {
            throw new IllegalArgumentException("Illegal pnfsid string length");
        }

        _a = new byte[length];

        System.arraycopy(idBytes, 0, _a, 0, _a.length);
        _idString = bytesToHexString(_a);
        _domain = null;
        _toString = _idString;
    }

    public boolean equals(Object o) {
        return (o instanceof PnfsId) && o.toString().equals(this.toString());
    }

    public int hashCode() {
        return toString().hashCode();
    }

    public int compareTo(PnfsId pnfsId) {

        if( pnfsId == this ) return 0;

        int i = 0;
        for (i = 0; (i < _a.length) && (_a[i] == pnfsId._a[i]); i++)
            ;
        if (i == _a.length)
            return 0;
        int t = _a[i] < 0 ? 256 + _a[i] : _a[i];
        int o = pnfsId._a[i] < 0 ? 256 + pnfsId._a[i] : pnfsId._a[i];

        return t < o ? -1 : 1;
    }

    public int intValue() {
        return hashCode();
    }

    public int getDatabaseId() {
        return (((_a[0]) & 0xFF) << 8) | ((_a[1]) & 0xFF);
    }

    public String getDomain() {
        return _domain;
    }

    public String getId() {
        return _idString;
    }

    public String toString() {
        return _toString;
    }

    public String toIdString() {
        return getId();
    }

    public static String toCompleteId(String shortId) {
        return bytesToHexString(_stringToBytes(shortId));
    }

    public byte[] getBytes() {
        byte[] x = new byte[_a.length];
        System.arraycopy(_a, 0, x, 0, _a.length);
        return x;
    }

    public String toShortString() {
        StringBuffer sb = new StringBuffer();
        int i = 0;
        for (i = 0; i < 2; i++)
            sb.append(byteToHexString(_a[i]));
        for (; (i < _a.length) && (_a[i] == 0); i++)
            ;
        for (; i < _a.length; i++)
            sb.append(byteToHexString(_a[i]));
        return sb.toString();
    }

    private static String byteToHexString(byte b) {
        String s = Integer.toHexString((b < 0) ? (256 + b) : (int) b)
            .toUpperCase();
        if (s.length() == 1)
            return "0" + s;
        else
            return s;
    }

    /**
     * Translation table used by bytesToHexString.
     */
    private static final char valueToHex[] = {
        '0', '1', '2', '3', '4', '5',
        '6', '7', '8', '9', 'A', 'B',
        'C', 'D', 'E', 'F' };

    /**
     * Converts a byte array into the string representation in base 16.
     */
    private static String bytesToHexString(byte[] b) {
        char result[] = new char[2 * b.length];
        for (int i = 0; i < b.length; i++) {
            int value = (b[i] + 0x100) & 0xFF;
            result[2 * i] = valueToHex[value >> 4];
            result[2 * i + 1] = valueToHex[value & 0x0F];
        }
        return new String(result);
    }

    private static byte[] _stringToBytes(String idString) {

        int len = idString.length();
        int idSize = 0;

        switch (len) {
        case OLD_ID_SIZE * 2: // old pnfsid
            idSize = OLD_ID_SIZE;
            break;
        case NEW_ID_SIZE * 2: // himera
            idSize = NEW_ID_SIZE;
            break;
        default:
            // all id's shorter than 24 characters will be extended to 24
            if ((len > OLD_ID_SIZE * 2) || (len == 0)) {
                throw new IllegalArgumentException(
                                                   "Illegal pnfsid string length");
            }
            idSize = OLD_ID_SIZE;
        }

        byte[] a = new byte[idSize];
        int p = idString.indexOf('*');
        if (p > -1) {
            if ((p == 0) || (p == (idString.length() - 1))) {
                throw new IllegalArgumentException("Illegal use of *");
            }
            int diff = 2 * OLD_ID_SIZE - idString.length() + 1;
            StringBuffer sb = new StringBuffer();
            sb.append(idString.substring(0, p));
            for (int i = 0; i < diff; i++)
                sb.append("0");
            sb.append(idString.substring(p + 1));
            idString = sb.toString();
        }
        if (idString.length() > (2 * a.length)) {
            throw new IllegalArgumentException("Illegal pnfsid string length");
        } else if (idString.length() < (2 * a.length)) {
            StringBuffer sb = new StringBuffer();
            int m = 2 * OLD_ID_SIZE - idString.length();
            for (int i = 0; i < m; i++)
                sb.append("0");
            sb.append(idString);
            idString = sb.toString();
        }
        for (int i = 0; i < idSize; i++) {
            int l = Integer
                .parseInt(idString.substring(2 * i, 2 * (i + 1)), 16);
            a[i] = (byte) ((l < 128) ? l : (l - 256));
        }
        return a;
    }

    /**
     * Converts string representation of pnfsid into its internal binary form
     *
     * @return pnfsid as byte array
     */
    public byte[] toBinPnfsId() {
        int len = _idString.length();
        switch (len) {
        case OLD_ID_SIZE * 2: // old pnfsid
            return PnfsIdUtil.toBinPnfsId(_idString);
        case NEW_ID_SIZE * 2: // himera
            return getBytes();
        default:
            return null;
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("USAGE : ... <pnfsId>");
            System.exit(4);
        }
        try {
            PnfsId id = new PnfsId(args[0]);
            System.out.println("id.toString()      " + id);
            System.out.println("id.getId()         " + id.getId());
            System.out.println("db.getDatabaseId() " + id.getDatabaseId());
            System.out.println("db.getDomain()     " + id.getDomain());
            System.out.println("id.getBytes()      " + java.util.Arrays.toString(id.getBytes()));
            System.out.println("id.toBinPnfsId()   " + java.util.Arrays.toString(id.toBinPnfsId()));
            System.out.println("toStringPnfsId(id.toBinPnfsId()) " + PnfsIdUtil.toStringPnfsId(id.toBinPnfsId()));
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(4);
        }
    }

}
