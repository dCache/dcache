package org.dcache.auth.attributes;

import java.io.Serializable;

/**
 * Immutable encapsulation of whether a user is marked as a read only
 * user.
 */
public class ReadOnly implements LoginAttribute, Serializable
{
    static final long serialVersionUID = -6487185735701329781L;

    private boolean _isReadOnly;

    public ReadOnly(boolean isReadOnly)
    {
        _isReadOnly = isReadOnly;
    }

    public boolean isReadOnly()
    {
        return _isReadOnly;
    }

    @Override
    public String toString()
    {
        return _isReadOnly ? "ReadOnly" : "ReadWrite";
    }
}