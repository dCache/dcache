// $Id$

package diskCacheV111.util;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.google.common.io.BaseEncoding;

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

    public PnfsId(String s) {
        this(stringToId(s), stringToDomain(s));
    }

    private PnfsId(byte[] id, String domain) {
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

    public String getId() {
        return BaseEncoding.base16().upperCase().encode(_a);
    }

    @Override
    public String toString() {
        return getId() + ((_domain != null) ? '.' + _domain : "");
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
