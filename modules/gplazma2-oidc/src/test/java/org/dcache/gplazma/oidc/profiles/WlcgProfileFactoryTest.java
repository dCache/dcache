/*
 * dCache - http://www.dcache.org/
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
package org.dcache.gplazma.oidc.profiles;

import diskCacheV111.util.FsPath;
import java.util.HashMap;
import java.util.Map;
import org.dcache.auth.GroupNamePrincipal;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class WlcgProfileFactoryTest {
    private WlcgProfileFactory factory;
    private WlcgProfile profile;

    @Before
    public void setup() {
        factory = new WlcgProfileFactory();
        profile = null;
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectProfileWithoutArguments() {
        when(factoryInvoked().withEmptyArgs());
    }

    @Test
    public void shouldCreateProfileWithPrefixArgument() {
        when(factoryInvoked().withArg("prefix", "/valid/absolute/path"));

        assertThat(profile.getPrefix(), is(equalTo(FsPath.create("/valid/absolute/path"))));
        assertThat(profile.getAuthzIdentity(), is(empty()));
        assertThat(profile.getNonAuthzIdentity(), is(empty()));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectProfileWithRelativePrefix() {
        when(factoryInvoked().withArg("prefix", "relative/path"));
    }

    @Test
    public void shouldCreateProfileWithAuthzIdentityArgument() {
        when(factoryInvoked()
                .withArg("prefix", "/valid/absolute/path")
                .withArg("authz-id", "group:my-group"));

        assertThat(profile.getAuthzIdentity(), hasItem(new GroupNamePrincipal("my-group")));
        assertThat(profile.getNonAuthzIdentity(), is(empty()));
    }

    @Test
    public void shouldCreateProfileWithNonAuthzIdentityArgument() {
        when(factoryInvoked()
                .withArg("prefix", "/valid/absolute/path")
                .withArg("non-authz-id", "group:my-group"));

        assertThat(profile.getAuthzIdentity(), is(empty()));
        assertThat(profile.getNonAuthzIdentity(), hasItem(new GroupNamePrincipal("my-group")));
    }

    @Test
    public void shouldCreateProfileWithBothAuthzAndNonAuthzIdentityArgument() {
        when(factoryInvoked()
                .withArg("prefix", "/valid/absolute/path")
                .withArg("authz-id", "group:authz-group")
                .withArg("non-authz-id", "group:non-authz-group"));

        assertThat(profile.getAuthzIdentity(), hasItem(new GroupNamePrincipal("authz-group")));
        assertThat(profile.getNonAuthzIdentity(), hasItem(new GroupNamePrincipal("non-authz-group")));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectBadAuthzIdentityArgument() {
        when(factoryInvoked()
                .withArg("prefix", "/valid/absolute/path")
                .withArg("authz-id", "invalid-principal"));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectBadNonAuthzIdentityArgument() {
        when(factoryInvoked()
                .withArg("prefix", "/valid/absolute/path")
                .withArg("non-authz-id", "invalid-principal"));
    }

    public void when(FactoryInvocation invocation) {
        profile = invocation.create();
    }

    private FactoryInvocation factoryInvoked() {
        return new FactoryInvocation();
    }

    private class FactoryInvocation {
        private final Map<String,String> arguments = new HashMap<>();

        public FactoryInvocation withArg(String key, String value) {
            arguments.put(key, value);
            return this;
        }

        public FactoryInvocation withEmptyArgs() {
            arguments.clear();
            return this;
        }

        public WlcgProfile create() {
            return factory.create(arguments);
        }
    }
}