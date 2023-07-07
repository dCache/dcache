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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.PasswordCredential;
import org.dcache.gplazma.AuthenticationException;
import org.junit.Before;
import org.junit.Test;


public class TokenExchangeTest {
 
    private TokenExchange plugin; 
    private Set<Principal> principals;
    private Set<Restriction> restrictions;

    @Before
    public void setup() {
        plugin = null;
        principals = null;
        restrictions = null;
    }

    // @Test
    // public void foo() {
    //     System.out.println("my first unit test");
    // }

    @Test(expected=AuthenticationException.class)
    public void shouldFailWithoutBearerToken() throws Exception {
        given(aPlugin());

        when(invoked().withoutCredentials());
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

        // public AuthenticateInvocationBuilder withBearerToken(String token) {
        //     privateCredentials.add(new BearerTokenCredential(token));
        //     return this;
        // }

        // public AuthenticateInvocationBuilder withUsernamePassword(String username, String password) {
        //     privateCredentials.add(new PasswordCredential(username, password));
        //     return this;
        // }

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