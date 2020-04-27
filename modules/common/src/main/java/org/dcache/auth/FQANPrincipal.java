package org.dcache.auth;

import java.io.Serializable;

/**
 * This class represents a fully qualified attribute
 * name (FQAN). FQANPrincipal is similar in purpose to
 * org.glite.security.voms.FQAN, except that the latter does not
 * implement the Principal and Serializable interfaces.
 * @since 2.1
 */
public class FQANPrincipal implements GroupPrincipal, Serializable
{
    private static final long serialVersionUID = -4242349585261079835L;

    private final FQAN _fqan;
    private final boolean _isPrimary;

    public FQANPrincipal(String fqan)
    {
        this(fqan, false);
    }

    public FQANPrincipal(String fqan, boolean isPrimary)
    {
        this(new FQAN(fqan), isPrimary);
    }

    public FQANPrincipal(FQAN fqan, boolean isPrimary)
    {
        if( fqan == null) {
            throw new IllegalArgumentException("null value not allowed");
        }
        _fqan = fqan;
        _isPrimary = isPrimary;
    }

    @Override
    public boolean isPrimaryGroup()
    {
        return _isPrimary;
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
        return _fqan.equals(other._fqan) && _isPrimary == other._isPrimary;
    }

    /** Returns the FQAN in string form. */
    @Override
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
        return _fqan.hashCode() ^ (_isPrimary ? 1 : 0);
    }

    @Override
    public String toString()
    {
        if (_isPrimary) {
            return FQANPrincipal.class.getSimpleName() + '[' + _fqan + ",primary]";
        } else {
            return FQANPrincipal.class.getSimpleName() + '[' + _fqan + ']';
        }
    }
}
