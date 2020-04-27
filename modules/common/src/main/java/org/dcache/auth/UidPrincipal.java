package org.dcache.auth;

import java.io.Serializable;
import java.security.Principal;

/**
 * This Principal represents the UID of a person.  In contrast to
 * LoginUidPrincipal, UidPrincipal represents an identity that the end-user
 * is allowed to adopt.  Therefore, it is safe to base authorisation
 * decisions on this principal.
 *
 * @see LoginUidPrincipal
 * @since 2.1
 */
public class UidPrincipal implements Principal, Serializable
{
    private static final long serialVersionUID = 1489893133915358418L;

    private final long _uid;

    public UidPrincipal(long uid) {
        if (uid < 0) {
            throw new IllegalArgumentException("UID must be non-negative");
        }
        _uid = uid;
    }

    public UidPrincipal(String uid) {
        this(Long.parseLong(uid));
    }

    public long getUid() {
        return _uid;
    }

    @Override
    public String getName() {
        return String.valueOf(_uid);
    }

    @Override
    public int hashCode() {
        return (int) _uid;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof UidPrincipal)) {
            return false;
        }
        UidPrincipal otherUid = (UidPrincipal) other;
        return (otherUid.getUid() == getUid());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '[' + getName() + ']';
    }
}
