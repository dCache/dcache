// $Id$

package diskCacheV111.util;

import com.google.common.base.Strings;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.google.common.io.BaseEncoding;

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Immutable representation of a pnfsId
 */
public class PnfsId implements Serializable, Comparable<PnfsId> {

    private static final String SIMPLE_PNFS_ID_REGEX = "\\p{XDigit}{1,24}";
    // FIXME STAR_PNFS_ID_REGEX is too permissive; overall length should be 24 or less.
    private static final String STAR_PNFS_ID_REGEX = "\\p{XDigit}{1,22}\\*\\p{XDigit}{1,22}";
    private static final String PNFS_ID_REGEX = '(' + SIMPLE_PNFS_ID_REGEX + '|' + STAR_PNFS_ID_REGEX + ')';
    private static final String CHIMERA_ID_REGEX = "\\p{XDigit}{36}";
    private static final String VALID_ID_REGEX = "^(" + PNFS_ID_REGEX + '|' + CHIMERA_ID_REGEX + ")$";
    private static final Pattern VALID_ID_PATTERN = Pattern.compile( VALID_ID_REGEX);

    /** Number of characters in a complete PNFS-style id String. */
    private static final int PNFS_ID_SIZE = 24;

    /** Number of characters in a Chimera-style id String. */
    private static final int CHIMERA_ID_SIZE = 36;

    private final byte[] _a;

    private static final long serialVersionUID = -112220393521303857L;

    public static boolean isValid( String id) {
        Matcher m = VALID_ID_PATTERN.matcher( id);
        return m.matches();
    }

    public PnfsId(String id) {
        checkArgument(!id.isEmpty(), "Empty PnfsId");

        String expandedId = expand(id);
        checkArgument(expandedId.length() == PNFS_ID_SIZE || expandedId.length() == CHIMERA_ID_SIZE,
                "Illegal pnfsid string length");
        _a = BaseEncoding.base16().decode(expandedId.toUpperCase());
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
        return Arrays.equals(_a, other._a);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(_a);
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

    @Override
    public String toString() {
        return BaseEncoding.base16().upperCase().encode(_a);
    }

    private static String expand(String id) {
        int p = id.indexOf('*');
        if (p > -1) {
            checkArgument(p > 0 && p < id.length()-1 && id.length()-1 <= PNFS_ID_SIZE,
                    "Illegal use of *");
            int count = PNFS_ID_SIZE - id.length() + 1;
            StringBuilder sb = new StringBuilder(PNFS_ID_SIZE).append(id.substring(0, p));
            for (int i = 0; i < count; i++) {
                sb.append('0');
            }
            return sb.append(id.substring(p+1)).toString();
        }

        return Strings.padStart(id, PNFS_ID_SIZE, '0');
    }

    public static Funnel<PnfsId> funnel()
    {
        return PnfsIdFunnel.INSTANCE;
    }

    private enum PnfsIdFunnel implements Funnel<PnfsId> {
        INSTANCE;

        @Override
        public void funnel(PnfsId from, PrimitiveSink into) {
            into.putBytes(from._a);
        }
    }
}
