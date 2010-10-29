package org.dcache.auth;

import java.security.Principal;
import java.io.Serializable;

public class LoginGidPrincipal implements GroupPrincipal, Serializable
{
    static final long serialVersionUID = -719644742571312959L;

    private long _gid;
    private boolean _isPrimary;

    public LoginGidPrincipal(long gid, boolean isPrimary)
    {
        if (gid < 0) {
            throw new IllegalArgumentException("GID must be non-negative");
        }
        _gid = gid;
        _isPrimary = isPrimary;
    }

    public LoginGidPrincipal(String gid, boolean isPrimary)
    {
        this(Long.parseLong(gid), isPrimary);
    }

    @Override
    public boolean isPrimaryGroup()
    {
        return _isPrimary;
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
        if (_isPrimary) {
            return (getClass().getSimpleName() + "[" + getName() + ",primary]");
        } else {
            return (getClass().getSimpleName() + "[" + getName() + "]");
        }
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
        return (other._gid == _gid && other._isPrimary == _isPrimary);
    }

    @Override
    public int hashCode() {
        return ((int) _gid ^ (_isPrimary ? 1 : 0));
    }
}
