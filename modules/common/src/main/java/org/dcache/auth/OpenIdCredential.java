package org.dcache.auth;

/**
 *  An OpenId Connect credential is composed of the following information.
 *
 *  {
 *      "access_token": "eyJraWQMCJ9.N_eoiDrnPTv46ZNj",
 *      "token_type": "Bearer",
 *      "refresh_token": "eyJhbGciO.",
 *      "expires_in": 3599,
 *      "scope": "openid offline_access profile email",
 *      "id_token": "Json Web Token"
 *  }
 *
 *  http://openid.net/specs/openid-connect-core-1_0.html
 *
 *  This interface exposes the methods necessary to obtain these information from a parsed OpenId Connect
 *  Credential.
 *
 *  The implementation of this interface is expected to hold information about the OpenId Connect credential
 *  as well as the information necessary to perform token exchange via delegaton. It is also suggested to
 *  hide the access token refresh within the implementation.
 *
 */
public interface OpenIdCredential
{
    String getBearerToken();

    String getAccessToken();

    long getExpiresAt();

    String getIssuedTokenType();

    String getRefreshToken();

    String getScope();

    String getTokenType();

    OpenIdClientSecret getClientCredential();

    String getOpenidProvider();
}
