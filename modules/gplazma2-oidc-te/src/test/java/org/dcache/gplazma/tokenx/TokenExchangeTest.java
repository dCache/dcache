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

import java.security.Principal;
import java.util.Base64;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;

import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.BearerTokenCredential;
import org.dcache.gplazma.AuthenticationException;
import org.junit.Before;
import org.junit.Test;


public class TokenExchangeTest {

    private HttpClient client = HttpClient.newHttpClient();
 
    private TokenExchange plugin; 
    private Set<Principal> principals;
    private Set<Restriction> restrictions;

    @Before
    public void setup() {
        plugin = null;
        principals = null;
        restrictions = null;
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailWithoutBearerToken() throws Exception {
        given(aPlugin());

        when(invoked().withoutCredentials());
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailWithTwoBearerTokens() throws Exception {
        given(aPlugin());

        when(invoked().withBearerToken("FOO").withBearerToken("BAR"));
    }

    @Test
    public void tokenExchangeTest() throws Exception {

        given(aPlugin());

        String helmholtzToken = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJiNWMzNTViNS0xY2NhLTRmZmEtYjRmNy02MDg4OGRhODc4ZmEiLCJhdWQiOiJwdWJsaWMtb2lkYy1hZ2VudCIsInNjb3BlIjoiZWR1cGVyc29uX2VudGl0bGVtZW50IGVtYWlsIGVkdXBlcnNvbl9zY29wZWRfYWZmaWxpYXRpb24gb2ZmbGluZV9hY2Nlc3MgcHJvZmlsZSBvcGVuaWQgZWR1cGVyc29uX3VuaXF1ZV9pZCIsImlzcyI6Imh0dHBzOlwvXC9sb2dpbi1kZXYuaGVsbWhvbHR6LmRlXC9vYXV0aDIiLCJleHAiOjE2OTc0NTA1MDYsImlhdCI6MTY5NzQ0NjUwNiwianRpIjoiYzM1MGI4NjYtOWMxNi00N2Q2LTg1ODMtMGY1YmYyMjQxZmU4IiwiY2xpZW50X2lkIjoicHVibGljLW9pZGMtYWdlbnQifQ.Ypifl_u_pBO2Kf65obgdzo-rbKSp35-GIq1fFk0nTTml9Ogl8LMY8wFAd-4Yhir3yCkvvDYzorzP8_NPkj_mvqxJRGGcku0Q80INTKTYew1eW4qlFhMqjRs9QCmVcpzYfmlBvlTfIYZ_oXr1SJAGJLfvzG2IyzGKr0_w3V-EgLEGuljhZAc9bibGbRn569_oX2n9TTqi-mGmJU72C4ssy88QK3WyieFGn0MQZdi95-WMyJG13Vo9qVAdgRdXTmOCzJdlvYLBRAWkUp6v9yEdxnbQb5REAakCirot1EBUOehpST_LrBjZCl0oQJa0kqrkpJKb7eejy9F1EISzuq7eTg";

        plugin.tokenExchange(helmholtzToken);


   }

//     @Test
//     public void prototyping() throws Exception {
//         System.out.println("=================================");
//         System.out.println("proto-typing:");

//         String helmholtzToken = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJiNWMzNTViNS0xY2NhLTRmZmEtYjRmNy02MDg4OGRhODc4ZmEiLCJhdWQiOiJwdWJsaWMtb2lkYy1hZ2VudCIsInNjb3BlIjoiZWR1cGVyc29uX2VudGl0bGVtZW50IGVtYWlsIGVkdXBlcnNvbl9zY29wZWRfYWZmaWxpYXRpb24gb2ZmbGluZV9hY2Nlc3MgcHJvZmlsZSBvcGVuaWQgZWR1cGVyc29uX3VuaXF1ZV9pZCIsImlzcyI6Imh0dHBzOlwvXC9sb2dpbi1kZXYuaGVsbWhvbHR6LmRlXC9vYXV0aDIiLCJleHAiOjE2OTc0NTA1MDYsImlhdCI6MTY5NzQ0NjUwNiwianRpIjoiYzM1MGI4NjYtOWMxNi00N2Q2LTg1ODMtMGY1YmYyMjQxZmU4IiwiY2xpZW50X2lkIjoicHVibGljLW9pZGMtYWdlbnQifQ.Ypifl_u_pBO2Kf65obgdzo-rbKSp35-GIq1fFk0nTTml9Ogl8LMY8wFAd-4Yhir3yCkvvDYzorzP8_NPkj_mvqxJRGGcku0Q80INTKTYew1eW4qlFhMqjRs9QCmVcpzYfmlBvlTfIYZ_oXr1SJAGJLfvzG2IyzGKr0_w3V-EgLEGuljhZAc9bibGbRn569_oX2n9TTqi-mGmJU72C4ssy88QK3WyieFGn0MQZdi95-WMyJG13Vo9qVAdgRdXTmOCzJdlvYLBRAWkUp6v9yEdxnbQb5REAakCirot1EBUOehpST_LrBjZCl0oQJa0kqrkpJKb7eejy9F1EISzuq7eTg";
//         String[] chunks = helmholtzToken.split("\\.");

//         System.out.println(helmholtzToken);

//         System.out.println(chunks.length);
//         System.out.println("0:" + chunks[0]);
//         System.out.println("1:" + chunks[1]);
//         System.out.println("2:" + chunks[2]);

//         String result = new String(Base64.getUrlDecoder().decode(chunks[1]));

//         System.out.println("=================================");
//         System.out.println("decode:");
//         System.out.println(Base64.getUrlDecoder().decode(chunks[0]));
//         System.out.println(Base64.getUrlDecoder().decode(chunks[1]));
//         System.out.println(result);

//     //     HttpRequest request = HttpRequest.newBuilder()
//     // .uri(URI.create("https://dev-keycloak.desy.de/auth/realms/desy-test/protocol/openid-connect/token"))
//     // .POST(BodyPublishers.ofString("client_id=exchange-test" + "&client_secret=S0iO4EcUyn0m4b4TgSqDYViDeo9vorAs\n" + //
//     //         "" + "&grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange&subject_token=" + helmholtzToken + "&subject_issuer=oidc&subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token&audience=exchange-test"))
//     // .setHeader("Content-Type", "application/x-www-form-urlencoded")
//     // .build();

//         HttpRequest request = HttpRequest.newBuilder()
//     .uri(URI.create("https://dev-keycloak.desy.de/auth/realms/desy-test/protocol/openid-connect/token"))
//     .POST(BodyPublishers.ofString("client_id=exchange-test-public"
//     + ""
//     + "&grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange"
//     + "&subject_token=" + helmholtzToken
//     + "&subject_issuer=oidc"
//     + "&subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token"
//     + "&audience=exchange-test"))
//     .setHeader("Content-Type", "application/x-www-form-urlencoded")
//     .build();

// HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

//         System.out.println(response);


//     }

    private void given(PluginBuilder builder) {
        plugin = builder.build();
    }

    private void when(AuthenticateInvocationBuilder builder) throws AuthenticationException {
        builder.invokeOn(plugin);
    }


    private PluginBuilder aPlugin() {
        return new PluginBuilder();
    }

    private AuthenticateInvocationBuilder invoked() {
        return new AuthenticateInvocationBuilder();
    }
    

    /**
     * A fluent class for building a (real) OidcAuthPlugin.
     */
    private class PluginBuilder {
        private Properties properties = new Properties();

        // public PluginBuilder() {
        //     // Use a reasonable default, just to keep tests a bit smaller.
        //     properties.setProperty("gplazma.oidc.audience-targets", "");
        // }

        // public PluginBuilder withTokenProcessor(TokenProcessorBuilder builder) {
        //     processor = builder.build();
        //     return this;
        // }

        // public PluginBuilder withProperty(String key, String value) {
        //     properties.setProperty(key, value);
        //     return this;
        // }


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

}