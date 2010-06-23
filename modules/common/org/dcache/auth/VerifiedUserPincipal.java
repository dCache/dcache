package org.dcache.auth;

import java.security.Principal;
import java.io.Serializable;

/**
 * This Principal represents the trustet username of a signed in
 * person that has already been verified/authenticated by a plugin.
 * @author jans
 */
public class VerifiedUserPincipal implements Principal, Serializable {

    static final long serialVersionUID = 1442698622147571301L;
    private String _username;

    public VerifiedUserPincipal(String username) {
        if (username == null) {
            throw new IllegalArgumentException();
        }
        _username = username;
    }

    @Override
    public String getName() {
        return _username;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof VerifiedUserPincipal)) {
            return false;
        }
        VerifiedUserPincipal otherUsername = (VerifiedUserPincipal) other;
        return (otherUsername.getName().equals(getName()));
    }

    @Override
    public int hashCode() {
        return _username.hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getName() + "]";
    }
}
