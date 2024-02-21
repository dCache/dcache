/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.gplazma.tokenx;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
// import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
// import java.net.URI;
// import java.net.http.HttpClient;
// import java.net.http.HttpRequest;
// import java.net.http.HttpRequest.BodyPublishers;
// import java.net.http.HttpResponse;

import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.BearerTokenCredential;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.ExtractResult;
// import org.dcache.gplazma.oidc.IdentityProvider;
// import org.dcache.gplazma.oidc.MockIdentityProviderBuilder;
import org.dcache.gplazma.oidc.TokenProcessor;
import org.dcache.gplazma.oidc.UnableToProcess;
// import org.dcache.gplazma.oidc.OidcAuthPluginTest;
// import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.BDDMockito;

// import static org.dcache.gplazma.oidc.MockIdentityProviderBuilder.anIP;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.google.common.base.Preconditions.checkState;

public class TokenExchangeTest {

    // private HttpClient client = HttpClient.newHttpClient();
 
    private TokenExchange plugin; 
    private Set<Principal> principals;
    private Set<Restriction> restrictions;
    private TokenProcessor processor;

    @Test
    public void testHello() {
        System.out.println("Hello");
        System.out.println("=================================[test_hello():]");
    }

     @Test
     public void tokenExchangeTest() throws Exception {

        System.out.println("=================================[tokenExchangeTest():");
        plugin = aPlugin().build();

        // given(aPlugin());

        // String helmholtzToken = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJiNWMzNTViNS0xY2NhLTRmZmEtYjRmNy02MDg4OGRhODc4ZmEiLCJhdWQiOiJwdWJsaWMtb2lkYy1hZ2VudCIsInNjb3BlIjoiZWR1cGVyc29uX2VudGl0bGVtZW50IGVtYWlsIGVkdXBlcnNvbl9zY29wZWRfYWZmaWxpYXRpb24gb2ZmbGluZV9hY2Nlc3MgcHJvZmlsZSBvcGVuaWQgZWR1cGVyc29uX3VuaXF1ZV9pZCIsImlzcyI6Imh0dHBzOlwvXC9sb2dpbi1kZXYuaGVsbWhvbHR6LmRlXC9vYXV0aDIiLCJleHAiOjE2OTc0NTA1MDYsImlhdCI6MTY5NzQ0NjUwNiwianRpIjoiYzM1MGI4NjYtOWMxNi00N2Q2LTg1ODMtMGY1YmYyMjQxZmU4IiwiY2xpZW50X2lkIjoicHVibGljLW9pZGMtYWdlbnQifQ.Ypifl_u_pBO2Kf65obgdzo-rbKSp35-GIq1fFk0nTTml9Ogl8LMY8wFAd-4Yhir3yCkvvDYzorzP8_NPkj_mvqxJRGGcku0Q80INTKTYew1eW4qlFhMqjRs9QCmVcpzYfmlBvlTfIYZ_oXr1SJAGJLfvzG2IyzGKr0_w3V-EgLEGuljhZAc9bibGbRn569_oX2n9TTqi-mGmJU72C4ssy88QK3WyieFGn0MQZdi95-WMyJG13Vo9qVAdgRdXTmOCzJdlvYLBRAWkUp6v9yEdxnbQb5REAakCirot1EBUOehpST_LrBjZCl0oQJa0kqrkpJKb7eejy9F1EISzuq7eTg";
        String helmholtzToken = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNWFkOWJlOC0zOThkLTQzNjMtYjhlYS05MDJmYjU1YWM3YzUiLCJhdWQiOiJwdWJsaWMtb2lkYy1hZ2VudCIsInNjb3BlIjoiZWR1cGVyc29uX2VudGl0bGVtZW50IHN5czpzY2ltOnJlYWRfcHJvZmlsZSBlbnRpdGxlbWVudHMgZWR1cGVyc29uX2Fzc3VyYW5jZSB2b3BlcnNvbl9leHRlcm5hbF9hZmZpbGlhdGlvbiBlZHVwZXJzb25fc2NvcGVkX2FmZmlsaWF0aW9uIGVkdXBlcnNvbl9wcmluY2lwYWxfbmFtZSBwcm9maWxlIHN5czpzY2ltOnJlYWRfbWVtYmVyc2hpcHMgY3JlZGVudGlhbHMgc2luZ2xlLWxvZ291dCBzbiBlbWFpbCBvZmZsaW5lX2FjY2VzcyBvcGVuaWQgZWR1cGVyc29uX3VuaXF1ZV9pZCBkaXNwbGF5X25hbWUgdm9wZXJzb25faWQgc3lzOnNjaW06cmVhZF9zZWxmX2dyb3VwIiwiaXNzIjoiaHR0cHM6XC9cL2xvZ2luLmhlbG1ob2x0ei5kZVwvb2F1dGgyIiwiZXhwIjoxNzA4NTExOTk2LCJpYXQiOjE3MDg1MDc5OTYsImp0aSI6IjBiNzFkNDFjLTEzZGItNDI3Ny05MWJiLWJjM2M0ZTQzMjVmMyIsImNsaWVudF9pZCI6InB1YmxpYy1vaWRjLWFnZW50In0.FWzCLhpV3yZAwZK_nMTlEvpBkKFTx-bDqNXYqmXXH-vbknQRMBUIirq6jqG00-QGtBTPsZQx9BCRmJA-G2u5lPj7PYmGScAIfoyUeiegixogtwYWOfpQrLrzhwOtazXVImEcjOIoADIcpPYblOunTqElzZ4Zd7fOVWLPPTZqVQTl3P7XUP0nBpdO8wH5KimtqXtMF4xS7Pf3BTohHm3uZGZM6zKsD0jfnAPLzbBaUqZ8_eq7OH6BRBl7xllKUIr8gcLXsDiB3X1GFeW1lk2TjTCt1DP3pXdyuDhLrlJS_UCiQYWuJkq3NE89hVTJ7sVTL-TXUGWU4LLKwPxKGsWLjg";
                 

        try {
            plugin.tokenExchange(helmholtzToken);
        }
        catch (IOException e) {
        
            // LOG.debug("Failed to parse token: {}", e.toString());
            System.out.println("Failed to parse token: " + e.toString());
        }

        System.out.println("=================================tokenExchangeTest():]");
    }

    private void given(PluginBuilder builder) {
        plugin = builder.build();
    }

    private void when(AuthenticateInvocationBuilder builder) throws AuthenticationException {
        builder.invokeOn(plugin);
    }


    private PluginBuilder aPlugin() {
        return new PluginBuilder();
    }

    private TokenProcessorBuilder aTokenProcessor() {
        return new TokenProcessorBuilder();
    }

    private AuthenticateInvocationBuilder invoked() {
        return new AuthenticateInvocationBuilder();
    }
    

    /**
     * A fluent class for building a (real) TokenExchange plugin.
     */
    private class PluginBuilder {
        private Properties properties = new Properties();

        public PluginBuilder() {
            // Use a reasonable default, just to keep tests a bit smaller.
            properties.setProperty("gplazma.oidc.audience-targets", "");
            // System.out.println("public PluginBuilder()");
        }

        public PluginBuilder withTokenProcessor(TokenProcessorBuilder builder) {
            processor = builder.build();
            return this;
        }

        public PluginBuilder withProperty(String key, String value) {
            properties.setProperty(key, value);
            return this;
        }

        public TokenExchange build() {
            return new TokenExchange(properties);
        }
    }

    /**
     * Fluent class to build an authentication plugin invocation.
     */
    private class AuthenticateInvocationBuilder {
        private final Set<Object> publicCredentials = new HashSet<>();
        private final Set<Object> privateCredentials = new HashSet<>();

        public AuthenticateInvocationBuilder withBearerToken(String token) {
            privateCredentials.add(new BearerTokenCredential(token));
            return this;
        }

        public AuthenticateInvocationBuilder withoutCredentials() {
            privateCredentials.clear();
            return this;
        }

        public void invokeOn(TokenExchange plugin) throws AuthenticationException {
            principals = new HashSet<>();
            restrictions = new HashSet<>();
            plugin.authenticate(publicCredentials, privateCredentials, principals, restrictions);
        }
    }

    /**
     * A fluent class for building a TokenProcessor.  It provides the same response for all extract
     * requests.
     */
    private static class TokenProcessorBuilder {
        private final TokenProcessor processor = mock(TokenProcessor.class);
        private boolean responseAdded;

        public TokenProcessorBuilder thatThrows(AuthenticationException exception) {
            checkState(!responseAdded);
            try {
                BDDMockito.given(processor.extract(ArgumentMatchers.any())).willThrow(exception);
            } catch (AuthenticationException | UnableToProcess e) {
                throw new RuntimeException("Impossible exception caught", e);
            }
            responseAdded = true;
            return this;
        }

        public TokenProcessorBuilder thatThrows(UnableToProcess exception) {
            checkState(!responseAdded);
            try {
                BDDMockito.given(processor.extract(ArgumentMatchers.any())).willThrow(exception);
            } catch (AuthenticationException | UnableToProcess e) {
                throw new RuntimeException("Impossible exception caught", e);
            }
            responseAdded = true;
            return this;
        }

        public TokenProcessorBuilder thatReturns(ExtractResultBuilder builder) {
            checkState(!responseAdded);
            try {
                BDDMockito.given(processor.extract(ArgumentMatchers.any())).willReturn(builder.build());
            } catch (AuthenticationException | UnableToProcess e) {
                throw new RuntimeException("Impossible exception caught", e);
            }
            responseAdded = true;
            return this;
        }

        public TokenProcessorBuilder thatFailsTestIfCalled() {
            checkState(!responseAdded);
            try {
                BDDMockito.given(processor.extract(ArgumentMatchers.any())).willThrow(new AssertionError("TokenProcessor#assert called"));
            } catch (AuthenticationException | UnableToProcess e) {
                throw new RuntimeException("Impossible exception caught", e);
            }
            responseAdded = true;
            return this;
        }

        public TokenProcessor build() {
            checkState(responseAdded);
            return processor;
        }
    }

    /**
     * A fluent class for building a mocked ExtractResult.
     */
    private static class ExtractResultBuilder {
        private final ExtractResult result = mock(ExtractResult.class);

        // public ExtractResultBuilder from(MockIdentityProviderBuilder builder) {
        //     var idp = builder.build();
        //     return from(idp);
        // }

        // public ExtractResultBuilder from(IdentityProvider idp) {
        //     BDDMockito.given(result.idp()).willReturn(idp);
        //     return this;
        // }

        // public ExtractResultBuilder containing(ClaimMapBuilder builder) {
        //     return containing(builder.build());
        // }

        // public ExtractResultBuilder containing(Map<String,JsonNode> claims) {
        //     BDDMockito.given(result.claims()).willReturn(claims);
        //     return this;
        // }

        // public ExtractResultBuilder withNoClaims() {
        //     // By default, Mockito returns an empty map.
        //     return this;
        // }

        public ExtractResult build() {
            return result;
        }
    }

     /**
     * A fluent class for building a set of claims.
     */
    private static class ClaimMapBuilder {
        private final ObjectMapper mapper = new ObjectMapper();
        private final Map<String,JsonNode> claims = new HashMap<>();

        public ClaimMapBuilder withStringClaim(String key, String value) {
            return with(key, "\"" + value + "\"");
        }

        public ClaimMapBuilder with(String key, String jsonValue) {
            try {
                JsonNode json = mapper.readTree(jsonValue);
                claims.put(key, json);
                return this;
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        public Map<String,JsonNode> build() {
            return claims;
        }
    }

}