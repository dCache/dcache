package org.dcache.auth;

import java.io.Serializable;
import java.security.Principal;

/**
 * The LoAPrincipal identifies what level of assurance (how certain it
 * is) that the user's identity, as described in the material used to
 * authenticate, accurately reflects the identity of the person issuing
 * the request.
 * @since 2.14
 */
public class LoAPrincipal implements Principal, Serializable
{
    private static final long serialVersionUID = 1L;

    private final LoA _loa;

    public LoAPrincipal(LoA loa)
    {
        _loa = loa;
    }

    @Override
    public String getName()
    {
        return _loa.getName();
    }

    public LoA getLoA()
    {
        return _loa;
    }

    @Override
    public int hashCode()
    {
        return _loa.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (!(other instanceof LoAPrincipal)) {
            return false;
        }

        LoAPrincipal otherLoA = (LoAPrincipal)other;

        return _loa == otherLoA._loa;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '[' + getName() + ']';
    }
}
