package org.dcache.auth.attributes;

import java.io.Serializable;

/**
 * Immutable encapsulation of a user's root directory. Used as session
 * data of a LoginReply.
 */
public class RootDirectory implements LoginAttribute, Serializable
{
    static final long serialVersionUID = 3092313442092927909L;

    private String _root;

    public RootDirectory(String root)
    {
        _root = root;
    }

    public String getRoot()
    {
        return _root;
    }
}