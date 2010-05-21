package org.dcache.auth;

import java.security.Principal;
import java.io.Serializable;

/**
 * This Principal represents the trustet username of a signed in
 * person. In contrast to a LoginName which is not yet authenticated
 * and points out a desire to become that name as a username.
 * @author jans
 */
public class UserNamePrincipal implements Principal, Serializable
{
    static final long serialVersionUID = 1447288627697571301L;

    private String _username;

    public UserNamePrincipal(String username) {
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
        if (!(other instanceof UserNamePrincipal)) {
            return false;
        }
        UserNamePrincipal otherUsername = (UserNamePrincipal) other;
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
