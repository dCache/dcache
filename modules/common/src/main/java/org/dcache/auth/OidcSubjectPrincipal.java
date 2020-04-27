package org.dcache.auth;

import com.google.common.base.CharMatcher;

import java.io.Serializable;
import java.security.Principal;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @since 2.16
 */
public class OidcSubjectPrincipal implements Principal, Serializable
{
    private static final long serialVersionUID = 1L;
    private final String _sub;

    public OidcSubjectPrincipal(String sub)
    {
        checkArgument(CharMatcher.ascii().matchesAllOf(sub), "OpenId \"sub\" is not ASCII encoded");
        checkArgument(sub.length() <= 255, "OpenId \"sub\" must not exceed 255 ASCII characters");
        _sub = sub;
    }

    @Override
    public String getName()
    {
        return _sub;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof OidcSubjectPrincipal)) {
            return false;
        }

        OidcSubjectPrincipal other = (OidcSubjectPrincipal) obj;
        return _sub.equals(other._sub);
    }

    @Override
    public int hashCode()
    {
        return _sub.hashCode();
    }

    @Override
    public String toString()
    {
        return "OidcSubjectPrincipal[" + _sub + ']';
    }
}
