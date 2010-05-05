package org.dcache.auth;

import java.security.Principal;

/**
 *
 * @author jans
 */
public class UidPrincipal implements Principal {

    private long _uid;

    public UidPrincipal(long uid) {
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
}
