// $Id$

package diskCacheV111.util;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Immutable representation of a pnfsId
 */
public class PnfsId implements Serializable, Comparable<PnfsId> {

    private static final String SIMPLE_PNFS_ID_REGEX = "\\p{XDigit}{1,24}";
    // FIXME STAR_PNFS_ID_REGEX is too permissive; overall length should be 24 or less.
    private static final String STAR_PNFS_ID_REGEX = "\\p{XDigit}{1,22}\\*\\p{XDigit}{1,22}";
    private static final String PNFS_ID_REGEX = '(' + SIMPLE_PNFS_ID_REGEX + '|' + STAR_PNFS_ID_REGEX + ')';
    private static final String PNFS_STRING_REGEX = '(' + PNFS_ID_REGEX + "(\\..*)?)";
    private static final String CHIMERA_ID_REGEX = "\\p{XDigit}{36}";
    private static final String VALID_ID_REGEX = "^(" + PNFS_STRING_REGEX + '|' + CHIMERA_ID_REGEX + ")$";
    private static final Pattern VALID_ID_PATTERN = Pattern.compile( VALID_ID_REGEX);

    private static final int OLD_ID_SIZE = 12; // original pnfs
    private static final int NEW_ID_SIZE = 18; // chimera

    private final byte[] _a;
    private final String _domain;

    private static final long serialVersionUID = -112220393521303857L;

    public static boolean isValid( String id) {
        Matcher m = VALID_ID_PATTERN.matcher( id);
        return m.matches();
    }

    public PnfsId(String id, String domain) {
        this(_stringToBytes(id), domain);
    }

    public PnfsId(String s) {
        this(stringToId(s), stringToDomain(s));
    }

    public PnfsId(byte[] id) {
        this(id, null);
    }

    public PnfsId(byte[] id, String domain) {
        int length = id.length;
        if (length != OLD_ID_SIZE && length != NEW_ID_SIZE) {
            throw new IllegalArgumentException("Illegal pnfsid string length");
        }
        _a = new byte[length];
        System.arraycopy(id, 0, _a, 0, length);

        _domain = (domain != null) ? domain.intern() : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof PnfsId)) {
            return false;
        }

        PnfsId other = (PnfsId) o;
        return Arrays.equals(_a, other._a)
            && Objects.equals(_domain, other._domain);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(_a) ^ ((_domain == null) ? 0 : _domain.hashCode());
    }

    @Override
    public int compareTo(PnfsId pnfsId) {
        if( pnfsId == this ) {
            return 0;
        }

        int i;
        for (i = 0; (i < _a.length) && (_a[i] == pnfsId._a[i]); i++) {
        }
        if (i == _a.length) {
            return 0;
        }
        int t = _a[i] < 0 ? 256 + _a[i] : _a[i];
        int o = pnfsId._a[i] < 0 ? 256 + pnfsId._a[i] : pnfsId._a[i];

        return t < o ? -1 : 1;
    }

    public int getDatabaseId() {
        return (((_a[0]) & 0xFF) << 8) | ((_a[1]) & 0xFF);
    }

    public String getDomain() {
        return _domain;
    }

    public String getId() {
        return bytesToHexString(_a);
    }

    @Override
    public String toString() {
        return getId() + ((_domain != null) ? '.' + _domain : "");
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
        StringBuilder sb = new StringBuilder();
        int i;
        for (i = 0; i < 2; i++) {
            sb.append(byteToHexString(_a[i]));
        }
        for (; (i < _a.length) && (_a[i] == 0); i++) {
        }
        for (; i < _a.length; i++) {
            sb.append(byteToHexString(_a[i]));
        }
        return sb.toString();
    }

    private static byte[] stringToId(String s) {
        int i = s.indexOf('.');
        if (i < 0) {
            return  _stringToBytes(s);
        } else {
            return _stringToBytes(s.substring(0, i));
        }
    }

    private static String stringToDomain(String s) {
        int i = s.indexOf('.');
        if (i < 0 || i == s.length() - 1) {
            return null;
        } else {
            return s.substring(i + 1);
        }
    }

    private static String byteToHexString(byte b) {
        String s = Integer.toHexString((b < 0) ? (256 + b) : (int) b)
            .toUpperCase();
        if (s.length() == 1) {
            return '0' + s;
        } else {
            return s;
        }
    }

    /**
     * Translation table used by bytesToHexString.
     */
    private static final char[] valueToHex = {
            '0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'A', 'B',
            'C', 'D', 'E', 'F'};

    /**
     * Converts a byte array into the string representation in base 16.
     */
    private static String bytesToHexString(byte[] b) {
        char[] result = new char[2 * b.length];
        for (int i = 0; i < b.length; i++) {
            int value = (b[i] + 0x100) & 0xFF;
            result[2 * i] = valueToHex[value >> 4];
            result[2 * i + 1] = valueToHex[value & 0x0F];
        }
        return new String(result);
    }

    private static byte[] _stringToBytes(String idString) {

        int len = idString.length();
        int idSize;

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
            StringBuilder sb = new StringBuilder();
            sb.append(idString.substring(0, p));
            for (int i = 0; i < diff; i++) {
                sb.append('0');
            }
            sb.append(idString.substring(p + 1));
            idString = sb.toString();
        }
        if (idString.length() > (2 * a.length)) {
            throw new IllegalArgumentException("Illegal pnfsid string length");
        } else if (idString.length() < (2 * a.length)) {
            StringBuilder sb = new StringBuilder();
            int m = 2 * OLD_ID_SIZE - idString.length();
            for (int i = 0; i < m; i++) {
                sb.append('0');
            }
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
        switch (_a.length) {
        case OLD_ID_SIZE: // old pnfsid
            return toBinPnfsId(getId());
        case NEW_ID_SIZE: // chimera
            return getBytes();
        default:
            return null;
        }
    }

    private static int hexToValue(byte val) {
        if (val >= '0' && val <= '9') {
            val -= '0';
            return val;
        } else if (val >= 'a' && val <= 'f') {
            val -= 'a' - 10;
            return val;
        } else if (val >= 'A' && val <= 'F') {
            val -= 'A' - 10;
            return val;
        }
        return -1;
    }


    /**
     * Converts string representation of pnfsid into its internal binary form used by PNFS server
     *
     * @param pnfsid as String
     * @return pnfsid as byte array
     */
    private static byte[] toBinPnfsId(String pnfsid) {
        byte[] bb = pnfsid.getBytes();
        byte[] ba = new byte[bb.length/2];
        //
        ba[ 0] = (byte)(hexToValue(bb[ 2])<<4 | hexToValue(bb[ 3]));
        ba[ 1] = (byte)(hexToValue(bb[ 0])<<4 | hexToValue(bb[ 1]));
        //
        ba[ 2] = (byte)(hexToValue(bb[ 6])<<4 | hexToValue(bb[ 7]));
        ba[ 3] = (byte)(hexToValue(bb[ 4])<<4 | hexToValue(bb[ 5]));
        //
        ba[ 4] = (byte)(hexToValue(bb[14])<<4 | hexToValue(bb[15]));
        ba[ 5] = (byte)(hexToValue(bb[12])<<4 | hexToValue(bb[13]));
        ba[ 6] = (byte)(hexToValue(bb[10])<<4 | hexToValue(bb[11]));
        ba[ 7] = (byte)(hexToValue(bb[ 8])<<4 | hexToValue(bb[ 9]));
        //
        ba[ 8] = (byte)(hexToValue(bb[22])<<4 | hexToValue(bb[23]));
        ba[ 9] = (byte)(hexToValue(bb[20])<<4 | hexToValue(bb[21]));
        ba[10] = (byte)(hexToValue(bb[18])<<4 | hexToValue(bb[19]));
        ba[11] = (byte)(hexToValue(bb[16])<<4 | hexToValue(bb[17]));
        return ba;
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
            System.out.println("id.getBytes()      " + Arrays.toString(id.getBytes()));
            System.out.println("id.toBinPnfsId()   " + Arrays.toString(id.toBinPnfsId()));
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(4);
        }
    }

    public static Funnel<PnfsId> funnel()
    {
        return PnfsIdFunnel.INSTANCE;
    }

    private enum PnfsIdFunnel implements Funnel<PnfsId> {
        INSTANCE;

        @Override
        public void funnel(PnfsId from, PrimitiveSink into) {
            if (from._domain != null) {
                into.putString(from._domain, StandardCharsets.US_ASCII);
            }
            into.putBytes(from._a);
        }
    }
}
