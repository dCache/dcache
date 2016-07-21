package org.dcache.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Objects;

public class StaticOpenIdCredential implements OpenIdCredential, Serializable
{
    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(StaticOpenIdCredential.class);

    private final String accessToken;
    private final long expiresAt;
    private final String issuedTokenType;
    private final String refreshToken;
    private final String scope;
    private final String tokenType;

    // Use to refresh Access Token
    private final OpenIdClientSecret clientCredential;
    private final String openidProvider;

    private StaticOpenIdCredential(Builder builder) {
        this.accessToken = builder._accessToken;
        this.expiresAt = builder._expiresAt;
        this.issuedTokenType = builder._issuedTokenType;
        this.refreshToken = builder._refreshToken;
        this.scope = builder._scope;
        this.tokenType = builder._tokenType;
        this.clientCredential = builder._clientCredential;
        this.openidProvider = builder._urlOpenidProvider;
    }

    @Override
    public String getAccessToken() {
        return accessToken;
    }

    @Override
    public long getExpiresAt() {
        return expiresAt;
    }

    @Override
    public String getIssuedTokenType() {
        return issuedTokenType;
    }

    @Override
    public String getRefreshToken() {
        return refreshToken;
    }

    @Override
    public String getScope() {
        return scope;
    }

    @Override
    public String getTokenType() {
        return tokenType;
    }

    @Override
    public OpenIdClientSecret getClientCredential() {
        return clientCredential;
    }

    @Override
    public String getOpenidProvider() {
        return openidProvider;
    }

    @Override
    public String getBearerToken() {
        return getAccessToken();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof StaticOpenIdCredential))
            return false;

        StaticOpenIdCredential that = (StaticOpenIdCredential) o;

        if (expiresAt != that.expiresAt)
            return false;
        if (!accessToken.equals(that.accessToken))
            return false;
        if (!issuedTokenType.equals(that.issuedTokenType))
            return false;
        if (!refreshToken.equals(that.refreshToken))
            return false;
        if (!scope.equals(that.scope))
            return false;
        if (!tokenType.equals(that.tokenType))
            return false;
        if (!clientCredential.equals(that.clientCredential))
            return false;
        return openidProvider.equals(that.openidProvider);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(accessToken,
                            expiresAt,
                            issuedTokenType,
                            refreshToken,
                            scope,
                            tokenType,
                            clientCredential, openidProvider);
    }

    public static Builder copyOf(OpenIdCredential credential)
    {
        return new Builder().accessToken(credential.getAccessToken())
                            .expiry(credential.getExpiresAt())
                            .refreshToken(credential.getRefreshToken())
                            .issuedTokenType(credential.getIssuedTokenType())
                            .scope(credential.getScope())
                            .tokenType(credential.getTokenType())
                            .clientCredential(credential.getClientCredential())
                            .provider(credential.getOpenidProvider());
    }

    public static class Builder
    {
        private String _accessToken = null;
        private long _expiresAt = 0L;
        private String _issuedTokenType = null;
        private String _refreshToken = null;
        private String _scope = null;
        private String _tokenType = null;
        private OpenIdClientSecret _clientCredential = null;
        private String _urlOpenidProvider = null;

        public Builder()
        {
        }

        public Builder accessToken(String accessToken)
        {
            this._accessToken = accessToken;
            return this;
        }

        public Builder expiry(long expiresIn)
        {
            this._expiresAt = System.currentTimeMillis() + expiresIn*1000L;
            return this;
        }

        public Builder refreshToken(String refreshToken)
        {
            this._refreshToken = refreshToken;
            return this;
        }

        public Builder issuedTokenType(String issuedTokenType)
        {
            this._issuedTokenType = issuedTokenType;
            return this;
        }

        public Builder scope(String scope)
        {
            this._scope = scope;
            return this;
        }

        public Builder tokenType(String tokenType)
        {
            this._tokenType = tokenType;
            return this;
        }

        public Builder clientCredential(OpenIdClientSecret clientCredential)
        {
            this._clientCredential = clientCredential;
            return this;
        }

        public Builder provider(String url)
        {
            this._urlOpenidProvider = url;
            return this;
        }

        public StaticOpenIdCredential build()
        {
            return new StaticOpenIdCredential(this);
        }
    }
}
