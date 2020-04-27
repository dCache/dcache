package org.dcache.auth;

import com.google.common.base.CharMatcher;

import java.io.Serializable;
import java.security.Principal;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The OpenId provider userInfo end-Point encodes information about groups to which a user belongs to.
 * This information about groups can be later mapped to gids within dCache.
 * @since 3.1
 */
public class OpenIdGroupPrincipal implements Principal, Serializable
{
    private final String group;

    public OpenIdGroupPrincipal(String group)
    {
        checkArgument(CharMatcher.ascii().matchesAllOf(group),
                     "OpenId \"group\": [%s] is not ASCII encoded", group);
        this.group = group;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (!(o instanceof OpenIdGroupPrincipal)) {
            return false;
        }

        OpenIdGroupPrincipal that = (OpenIdGroupPrincipal) o;

        return group.equals(that.group);

    }

    @Override
    public int hashCode() {
        return group.hashCode();
    }

    @Override
    public String getName() {
        return group;
    }

    @Override
    public String toString() {
        return "OpenIdGroup[" + getName() + ']';
    }
}
