package org.dcache.auth;

import java.io.Serializable;
import java.security.Principal;

/**
 * This class represents a fully qualified attribute
 * name. FQANPrincipal is similar in purpose to
 * org.glite.security.voms.FQAN, except that the latter does not
 * implement the Principal and Serializable interfaces.
 */
public class FQANPrincipal implements Principal, Serializable
{
    private final String _fqan;
    private final boolean _primary;

    public FQANPrincipal(String fqan, boolean primary)
    {
        if (fqan == null) {
            throw new IllegalArgumentException("null value not allowed");
        }
        _fqan = fqan;
        _primary = primary;
    }

    public boolean isPrimary()
    {
        return _primary;
    }

    public boolean equals(Object another)
    {
        if (another == this) {
            return true;
        }
        if (!(another instanceof FQANPrincipal)) {
            return false;
        }

        FQANPrincipal other = (FQANPrincipal) another;
        return _fqan.equals(other._fqan) && _primary == other._primary;
    }

    /** Returns the FQAN in string form. */
    public String getName()
    {
        return _fqan;
    }

    public int hashCode()
    {
        return _fqan.hashCode();
    }

    public String toString()
    {
        return _fqan;
    }
}