package org.dcache.auth;

import java.io.Serializable;

/**
 * The class PasswordCredential acts as a holder for username and
 * password.
 */
public class PasswordCredential implements Serializable
{
    private static final long serialVersionUID = -8823304503972043526L;

    private final String _username;
    private final String _password;

    public PasswordCredential(String username, String password)
    {
        _username = username;
        _password = password;
    }

    public String getUsername()
    {
        return _username;
    }

    public String getPassword()
    {
        return _password;
    }

    @Override
    public String toString()
    {
        return PasswordCredential.class.getSimpleName() + "[user=" + _username + ']';
    }
}
