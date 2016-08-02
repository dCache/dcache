package org.dcache.auth;

import com.google.common.base.CharMatcher;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;

public class BearerTokenCredential implements Serializable
{
    private static final long serialVersionUID = -5933313664563503235L;
    private final String _token;

    public BearerTokenCredential(String token)
    {
        checkArgument(CharMatcher.ASCII.matchesAllOf(token), "Bearer Token not ASCII");
        _token = token;
    }

    public String getToken()
    {
        return _token;
    }

    @Override
    public String toString()
    {
        return BearerTokenCredential.class.getSimpleName() + "[bearerToken=" + _token + ']';
    }
}
