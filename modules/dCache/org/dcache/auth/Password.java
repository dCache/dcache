package org.dcache.auth;

import java.io.Serializable;

public class Password implements Serializable
{
    static final long serialVersionUID = -8823304503972043526L;

    private String _password;

    public Password(String password)
    {
        _password = password;
    }

    public String getPassword()
    {
        return _password;
    }

    public String toString()
    {
        return Password.class.getSimpleName();
    }
}
