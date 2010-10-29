package org.dcache.auth;

import java.security.Principal;
import java.io.Serializable;

/**
 * This Principal represents the GID of a person.
 */
public class GidPrincipal implements GroupPrincipal, Serializable
{
    static final long serialVersionUID = 7812225739755920892L;

    private long _gid;
    private boolean _isPrimaryGroup;

    public GidPrincipal(long gid, boolean isPrimary) {
        if (gid < 0) {
            throw new IllegalArgumentException("GID must be non-negative");
        }
        _gid = gid;
        _isPrimaryGroup = isPrimary;
    }

    public GidPrincipal(String gid, boolean isPrimary) {
        this(Long.parseLong(gid), isPrimary);
    }

    @Override
    public boolean isPrimaryGroup() {
        return _isPrimaryGroup;
    }

    public long getGid() {
        return _gid;
    }

    @Override
    public String getName() {
        return String.valueOf(_gid);
    }

    @Override
    public String toString() {
        if (_isPrimaryGroup) {
            return (getClass().getSimpleName() + "[" + getName() + ",primary]");
        } else {
            return (getClass().getSimpleName() + "[" + getName() + "]");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof GidPrincipal)) {
            return false;
        }
        GidPrincipal otherGid = (GidPrincipal) other;
        return (otherGid.getGid() == getGid() &&
                (otherGid.isPrimaryGroup() == isPrimaryGroup()));
    }

    @Override
    public int hashCode() {
        return ((int) _gid + (_isPrimaryGroup ? 1 : 0));
    }
}
