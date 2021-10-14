package org.dcache.auth;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.base.CharMatcher;
import java.io.Serializable;
import java.security.Principal;

/**
 * @since 2.16
 */
@AuthenticationOutput
public class OidcSubjectPrincipal implements Principal, Serializable {

    private static final long serialVersionUID = 1L;
    private final String _sub;
    private final String _op;

    /**
     * Create a new principal.
     *
     * @param sub The value of the 'sub' claim.
     * @param op  The name/alias of the OP that asserted this claim.
     */
    public OidcSubjectPrincipal(String sub, String op) {
        checkArgument(CharMatcher.ascii().matchesAllOf(sub), "OpenId \"sub\" is not ASCII encoded");
        checkArgument(sub.length() <= 255, "OpenId \"sub\" must not exceed 255 ASCII characters");
        _sub = sub;
        _op = requireNonNull(op);
    }

    @Override
    public String getName() {
        return _sub + "@" + _op;
    }

    /**
     * @return the value of the 'sub' claim.
     */
    public String getSubClaim() {
        return _sub;
    }

    /**
     * @return the dCache-internal alias for the OP.
     */
    public String getOP() {
        return _op;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof OidcSubjectPrincipal)) {
            return false;
        }

        OidcSubjectPrincipal other = (OidcSubjectPrincipal) obj;
        return _sub.equals(other._sub) && _op.equals(other._op);
    }

    @Override
    public int hashCode() {
        return _sub.hashCode() ^ _op.hashCode();
    }

    @Override
    public String toString() {
        return "OidcSubjectPrincipal[" + _sub + '@' + _op + ']';
    }
}
