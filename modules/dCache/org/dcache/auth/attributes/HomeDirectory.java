package org.dcache.auth.attributes;

import java.io.Serializable;

/**
 * Encapsulation of a user's home. Used as session data of a
 * LoginReply.
 */
public class HomeDirectory implements LoginAttribute, Serializable
{
    static final long serialVersionUID = -1502727254247340036L;

    private String _home;

    public HomeDirectory(String home)
    {
        _home = home;
    }

    public String getHome()
    {
        return _home;
    }

    @Override
    public String toString()
    {
        return "HomeDirectory[" + _home + "]";
    }
}