package org.dcache.auth;

import java.io.Serializable;
import java.security.Principal;

/**
 * This principal represents an untrusted GID: the GID that the end-client
 * wishes to become.  Typically no checks are made that the end-user is
 * actually a member of this group; therefore it is recommended not to make
 * authorisation decisions based on this principal.
 *
 * @see GidPrincipal
 * @since 2.1
 */
public class LoginGidPrincipal implements Principal, Serializable
{
    private static final long serialVersionUID = -719644742571312959L;

    private final long _gid;

    public LoginGidPrincipal(long gid)
    {
        if (gid < 0) {
            throw new IllegalArgumentException("GID must be non-negative");
        }
        _gid = gid;
    }

    public LoginGidPrincipal(String gid)
    {
        this(Long.parseLong(gid));
    }

    public long getGid()
    {
        return _gid;
    }

    @Override
    public String getName()
    {
        return String.valueOf(_gid);
    }

    @Override
    public String toString()
    {
        return (getClass().getSimpleName() + '[' + getName() + ']');
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof LoginGidPrincipal)) {
            return false;
        }
        LoginGidPrincipal other = (LoginGidPrincipal) obj;
        return (other._gid == _gid);
    }

    @Override
    public int hashCode() {
        return (int) _gid;
    }
}
