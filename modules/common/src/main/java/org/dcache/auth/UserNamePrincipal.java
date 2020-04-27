package org.dcache.auth;

import java.io.Serializable;
import java.security.Principal;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This Principal represents the trusted username of a signed in
 * person. This is in contrast to a LoginNamePrincipal.
 *
 * @see LoginNamePrincipal
 * @since 2.1
 */
public class UserNamePrincipal implements Principal, Serializable
{
    private static final long serialVersionUID = 1447288627697571301L;

    private final String _username;

    public UserNamePrincipal(String username) {
        checkArgument(!username.isEmpty(), "Username can't be an empty string");
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
        return getClass().getSimpleName() + '[' + getName() + ']';
    }
}
