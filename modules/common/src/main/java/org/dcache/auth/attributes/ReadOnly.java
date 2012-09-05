package org.dcache.auth.attributes;

import java.io.Serializable;

/**
 * Immutable encapsulation of whether a user is marked as a read only
 * user.
 */
public class ReadOnly implements LoginAttribute, Serializable
{
    private static final long serialVersionUID = -6487185735701329781L;

    private boolean _isReadOnly;

    public ReadOnly(boolean isReadOnly)
    {
        _isReadOnly = isReadOnly;
    }

    public ReadOnly(String readOnly)
    {
        this(Boolean.valueOf(readOnly));
    }

    public boolean isReadOnly()
    {
        return _isReadOnly;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ReadOnly)) {
            return false;
        }
        ReadOnly other = (ReadOnly) obj;
        return _isReadOnly == other._isReadOnly;
    }

    @Override
    public int hashCode()
    {
        return _isReadOnly ? 1 : 0;
    }

    @Override
    public String toString()
    {
        return _isReadOnly ? "ReadOnly" : "ReadWrite";
    }
}