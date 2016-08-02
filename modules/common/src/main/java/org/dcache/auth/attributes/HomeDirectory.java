package org.dcache.auth.attributes;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Encapsulation of a user's home. Used as session data of a
 * LoginReply.
 */
public class HomeDirectory implements LoginAttribute, Serializable
{
    private static final long serialVersionUID = -1502727254247340036L;

    private final String _home;

    public HomeDirectory(String home)
    {
        checkNotNull(home);
        _home = home;
    }

    public String getHome()
    {
        return _home;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HomeDirectory)) {
            return false;
        }
        HomeDirectory other = (HomeDirectory) obj;
        return _home.equals(other._home);
    }

    @Override
    public int hashCode()
    {
        return _home.hashCode();
    }

    @Override
    public String toString()
    {
        return "HomeDirectory[" + _home + ']';
    }
}
