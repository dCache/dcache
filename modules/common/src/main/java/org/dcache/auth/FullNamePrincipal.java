package org.dcache.auth;

import com.google.common.base.Joiner;

import java.io.Serializable;
import java.security.Principal;

import static com.google.common.base.Preconditions.checkArgument;

public class FullNamePrincipal implements Principal, Serializable
{
    private static final long serialVersionUID = 1L;
    private final String _fullName;

    public FullNamePrincipal(String fullName)
    {
        checkArgument(!fullName.isEmpty(), "Full Name not given");
        _fullName = fullName;
    }

    public FullNamePrincipal(String givenName, String familyName)
    {
        checkArgument(!givenName.isEmpty() || !familyName.isEmpty(), "No Names given");
        _fullName = Joiner.on(' ').skipNulls().join(givenName, familyName).trim();
    }

    @Override
    public String getName()
    {
        return _fullName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof FullNamePrincipal))
            return false;

        FullNamePrincipal that = (FullNamePrincipal) o;

        return _fullName.equals(that._fullName);

    }

    @Override
    public int hashCode() {
        return _fullName.hashCode();
    }

    @Override
    public String toString() {
        return "FullNamePrincipal[" +_fullName + ']';
    }
}
