package org.dcache.auth;

public class WrappingOpenIdCredential implements OpenIdCredential {
    protected OpenIdCredential credential;

    public WrappingOpenIdCredential(OpenIdCredential credential) {
        this.credential = credential;
    }

    @Override
    public String getBearerToken() {
        return credential.getBearerToken();
    }

    @Override
    public String getAccessToken() {
        return credential.getAccessToken();
    }

    @Override
    public long getExpiresAt() {
        return credential.getExpiresAt();
    }

    @Override
    public String getIssuedTokenType() {
        return credential.getIssuedTokenType();
    }

    @Override
    public String getRefreshToken() {
        return credential.getRefreshToken();
    }

    @Override
    public String getScope() {
        return credential.getScope();
    }

    @Override
    public String getTokenType() {
        return credential.getTokenType();
    }

    @Override
    public OpenIdClientSecret getClientCredential() {
        return credential.getClientCredential();
    }

    @Override
    public String getOpenidProvider() {
        return credential.getOpenidProvider();
    }
}
