package org.dcache.auth;

import java.io.Serializable;
import java.security.Principal;

/**
 * This class represents a fully qualified attribute
 * name (FQAN). FQANPrincipal is similar in purpose to
 * org.glite.security.voms.FQAN, except that the latter does not
 * implement the Principal and Serializable interfaces.
 */
public class FQANPrincipal implements GroupPrincipal, Serializable
{
    private static final long serialVersionUID = -4242349585261079835L;

    private FQAN _fqan;
    private boolean _primary;

    public FQANPrincipal(String fqan)
    {
        this(fqan, false);
    }

    public FQANPrincipal(String fqan, boolean primary)
    {
        this(new FQAN(fqan), primary);
    }

    public FQANPrincipal(FQAN fqan, boolean primary)
    {
        if( fqan == null) {
            throw new IllegalArgumentException("null value not allowed");
        }
        _fqan = fqan;
        _primary = primary;
    }

    @Override
    public boolean isPrimaryGroup()
    {
        return _primary;
    }

    @Override
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
        return _fqan.toString();
    }

    public FQAN getFqan()
    {
        return _fqan;
    }

    @Override
    public int hashCode()
    {
        return _fqan.hashCode();
    }

    @Override
    public String toString()
    {
        return FQANPrincipal.class.getSimpleName() + "[" + _fqan + "]";
    }
}