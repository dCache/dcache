package org.dcache.auth;

import java.io.Serializable;

/**
 * @since 2.1
 */
public class GroupNamePrincipal implements GroupPrincipal, Serializable
{
    private static final long serialVersionUID = -9202753005930409597L;

    private final String _name;
    private final boolean _isPrimary;

    public GroupNamePrincipal(String name)
    {
        this(name, false);
    }

    public GroupNamePrincipal(String name, boolean isPrimary)
    {
        if (name == null) {
            throw new NullPointerException();
        }
        _name = name;
        _isPrimary = isPrimary;
    }

    @Override
    public boolean isPrimaryGroup()
    {
        return _isPrimary;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GroupNamePrincipal)) {
            return false;
        }
        GroupNamePrincipal otherName = (GroupNamePrincipal) other;
        return
            otherName.getName().equals(getName()) &&
            otherName.isPrimaryGroup() == isPrimaryGroup();
    }

    @Override
    public int hashCode()
    {
        return _name.hashCode() ^ (_isPrimary ? 1 : 0);
    }

    @Override
    public String toString()
    {
        if (_isPrimary) {
            return GroupNamePrincipal.class.getSimpleName() + '[' + getName() + ",primary]";
        } else {
            return GroupNamePrincipal.class.getSimpleName() + '[' + getName() + ']';
        }
    }
}
