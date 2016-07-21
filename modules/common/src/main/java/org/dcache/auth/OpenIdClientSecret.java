package org.dcache.auth;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

public class OpenIdClientSecret implements Serializable
{
    private final String id;
    private final String secret;

    public OpenIdClientSecret(String id, String secret) {
        this.id = checkNotNull(id);
        this.secret = checkNotNull(secret);
    }

    public String getId() {
        return id;
    }

    public String getSecret() {
        return secret;
    }
}
