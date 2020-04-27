package org.dcache.auth;

import java.io.Serializable;
import java.security.Principal;

/**
 * This Principal represents the GID of a person.  The GID represents a group
 * that the user is a member of, possibly in common with other users.  A
 * GidPrincipal is either the primary GID or is not.
 *
 * Typical login requirements are that a user has at least one GidPrincipal and
 * precisely one primary GidPrincipal.
 *
 * The GidPrincipal represents a vetted identity information about the user.
 * This is in contrast to LoginGidPrincipal.  Therefore, it is safe to base
 * authorisation decisions on the presence of GidPrincipal.
 *
 * @see LoginGidPrincipal
 * @since 2.1
 */
public class GidPrincipal implements GroupPrincipal, Serializable
{
    private static final long serialVersionUID = 7812225739755920892L;

    private final long _gid;
    private final boolean _isPrimaryGroup;

    public static boolean isPrimaryGid(Principal principal) {
        return principal instanceof GidPrincipal && ((GidPrincipal)principal).isPrimaryGroup();
    }

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

    public GidPrincipal withPrimaryGroup(boolean isPrimaryGroup) {
        return isPrimaryGroup == _isPrimaryGroup
                ? this
                : new GidPrincipal(_gid, isPrimaryGroup);
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
            return (getClass().getSimpleName() + '[' + getName() + ",primary]");
        } else {
            return (getClass().getSimpleName() + '[' + getName() + ']');
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
