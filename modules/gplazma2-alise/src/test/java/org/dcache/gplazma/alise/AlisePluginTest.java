/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2024 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.alise;

import java.net.URI;
import java.security.Principal;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.util.PrincipalSetMaker;
import org.dcache.util.Result;
import org.junit.Test;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import java.util.HashSet;
import java.util.List;
import org.dcache.auth.FullNamePrincipal;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AlisePluginTest {

    private AlisePlugin plugin;
    private Set<Principal> principals;
    private LookupAgent agent;
    private List<Identity> lookupCalls;

    @Before
    public void setup() {
        plugin = null;
        principals = null;
        agent = null;
        lookupCalls = null;
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfNoSubClaim() throws Exception {
        given(anAlisePlugin()
                .withProprety("gplazma.alise.issuers", "")
                .withAgent(aLookupAgent().thatFailsTestIfCalled()));

        whenPluginMapWith(principals().withDn("/O=ACME/CN=Wile E Coyote"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfMissingOpPrincipal() throws Exception {
        given(anAlisePlugin()
                .withProprety("gplazma.alise.issuers", "")
                .withAgent(aLookupAgent().thatFailsTestIfCalled()));

        whenPluginMapWith(principals().withOidc("paul", "EXAMPLE-OP"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfOpNotListedByAlias() throws Exception {
        given(anAlisePlugin()
                .withProprety("gplazma.alise.issuers", "EXAMPLE-OP")
                .withAgent(aLookupAgent().thatFailsTestIfCalled()));

        whenPluginMapWith(principals().withOidc("paul", "SOME-OTHER-OP"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfOpNotListedByUri() throws Exception {
        given(anAlisePlugin()
                .withProprety("gplazma.alise.issuers", "https://issuer.example.org/")
                .withAgent(aLookupAgent().thatFailsTestIfCalled()));

        whenPluginMapWith(principals().withOidc("paul", "SOME-OTHER-OP"));
    }

    @Test
    public void shouldAcceptSuccessfulLookupWithUsername() throws Exception {
        given(anAlisePlugin()
                .withProprety("gplazma.alise.issuers", "")
                .withAgent(aLookupAgent().thatReturnsSuccess(principals().withUsername("paul"))));

        whenPluginMapWith(principals()
                .withOidc("paul", "EXAMPLE-OP")
                .withOauth2Provider("EXAMPLE-OP", URI.create("https://issuer.example.org/")));

        assertThat(lookupCalls, contains(new Identity(URI.create("https://issuer.example.org/"), "paul")));
        assertThat(principals, hasItem(new UserNamePrincipal("paul")));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfLookupFails() throws Exception {
        given(anAlisePlugin()
                .withProprety("gplazma.alise.issuers", "")
                .withAgent(aLookupAgent().thatReturnsFailure("fnord")));

        try {
            whenPluginMapWith(principals()
                    .withOidc("paul", "EXAMPLE-OP")
                    .withOauth2Provider("EXAMPLE-OP", URI.create("https://issuer.example.org/")));
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("fnord"));
            throw e;
        }
    }

    @Test
    public void shouldAcceptSuccessfulLookupWithUsernameAndFullname() throws Exception {
        given(anAlisePlugin()
                .withProprety("gplazma.alise.issuers", "")
                .withAgent(aLookupAgent().thatReturnsSuccess(principals()
                        .withUsername("paul")
                        .withFullname("Paul Millar"))));

        whenPluginMapWith(principals()
                .withOidc("paul", "EXAMPLE-OP")
                .withOauth2Provider("EXAMPLE-OP", URI.create("https://issuer.example.org/")));

        assertThat(lookupCalls, contains(new Identity(URI.create("https://issuer.example.org/"), "paul")));
        assertThat(principals, hasItems(new UserNamePrincipal("paul"),
                new FullNamePrincipal("Paul Millar")));
    }

    @Test
    public void shouldFilterSuppressedOPByIssuerAlias() throws Exception {
        given(anAlisePlugin()
                .withProprety("gplazma.alise.issuers", "EXAMPLE-OP")
                .withAgent(aLookupAgent().thatReturnsSuccess(principals()
                        .withUsername("paul"))));

        whenPluginMapWith(principals()
                .withOidc("paul", "EXAMPLE-OP")
                .withOauth2Provider("EXAMPLE-OP", URI.create("https://issuer.example.org/"))
                .withOidc("paul", "SOME-OTHER-OP")
                .withOauth2Provider("SOME-OTHER-OP", URI.create("https://some-other-issuer.example.com/")));

        assertThat(lookupCalls, contains(new Identity(URI.create("https://issuer.example.org/"), "paul")));
        assertThat(principals, hasItems(new UserNamePrincipal("paul")));
    }

    @Test
    public void shouldFilterSuppressedOPByIssuerUri() throws Exception {
        given(anAlisePlugin()
                .withProprety("gplazma.alise.issuers", "https://issuer.example.org/")
                .withAgent(aLookupAgent().thatReturnsSuccess(principals()
                        .withUsername("paul"))));

        whenPluginMapWith(principals()
                .withOidc("paul", "EXAMPLE-OP")
                .withOauth2Provider("EXAMPLE-OP", URI.create("https://issuer.example.org/"))
                .withOidc("paul", "SOME-OTHER-OP")
                .withOauth2Provider("SOME-OTHER-OP", URI.create("https://some-other-issuer.example.com/")));

        assertThat(lookupCalls, contains(new Identity(URI.create("https://issuer.example.org/"), "paul")));
        assertThat(principals, hasItems(new UserNamePrincipal("paul")));
    }

    private AlisePluginBuilder anAlisePlugin() {
        return new AlisePluginBuilder();
    }

    private LookupAgentBuilder aLookupAgent() {
        return new LookupAgentBuilder();
    }

    private PrincipalSetMaker principals() {
        return new PrincipalSetMaker();
    }

    private void given(AlisePluginBuilder builder) {
        plugin = builder.build();
    }

    private void whenPluginMapWith(PrincipalSetMaker maker) throws AuthenticationException {
        // PrincipalSetMaker creates an unmodifiable Set; but, for gPlazma, we need a Set that can be modified.
        principals = new HashSet<>(maker.build());

        plugin.map(principals);

        ArgumentCaptor<Identity> identityCaptor = ArgumentCaptor.forClass(Identity.class);
        verify(agent).lookup(identityCaptor.capture());
        lookupCalls = identityCaptor.getAllValues();
    }

    private class AlisePluginBuilder {
        private final Properties config = new Properties();

        private AlisePluginBuilder withProprety(String key, String value) {
            config.setProperty(key, value);
            return this;
        }

        private AlisePluginBuilder withAgent(LookupAgentBuilder builder) {
            agent = builder.build();
            return this;
        }

        private AlisePlugin build() {
            checkState(agent != null);
            return new AlisePlugin(config, agent);
        }
    }

    private static class LookupAgentBuilder {
        private final LookupAgent agent = mock(LookupAgent.class);
        private Result<Collection<Principal>,String> result;
        private boolean failTest;

        private LookupAgentBuilder thatReturnsFailure(String failure) {
            checkState(result == null);
            checkState(!failTest);
            result = Result.failure(requireNonNull(failure));
            return this;
        }

        private LookupAgentBuilder thatReturnsSuccess(PrincipalSetMaker maker) {
            checkState(result == null);
            checkState(!failTest);
            result = Result.success(maker.build());
            return this;
        }

        private LookupAgentBuilder thatFailsTestIfCalled() {
            checkState(result == null);
            failTest = true;
            return this;
        }

        private LookupAgent build() {
            checkState(result != null || failTest);
            if (failTest) {
                when(agent.lookup(any())).thenThrow(new AssertionError("Unexpected call to LookupAgent#lookup"));
            } else {
                when(agent.lookup(any())).thenReturn(result);
            }
            return agent;
        }
    }
}
