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

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class OidcProfileFactoryTest {

    private OidcProfileFactory factory;
    private OidcProfile profile;

    @Before
    public void setup() {
        factory = new OidcProfileFactory();
        profile = null;
    }

    public OidcProfileFactoryTest() {}

    @Test
    public void shouldCreateWithout() {
        when(factoryInvoked().withEmptyArgs());

        assertThat(profile.isPreferredUsernameClaimAccepted(), is(false));
        assertThat(profile.isGroupsClaimMappedToGroupName(), is(false));
    }


    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectUnsupportedAccept() {
        when(factoryInvoked().withArg("accept", "bad-value"));
    }

    @Test
    public void shouldCreateWithAcceptUsername() {
        when(factoryInvoked().withArg("accept", "username"));

        assertThat(profile.isPreferredUsernameClaimAccepted(), is(true));
        assertThat(profile.isGroupsClaimMappedToGroupName(), is(false));
    }

    @Test
    public void shouldCreateWithAcceptGroups() {
        when(factoryInvoked().withArg("accept", "groups"));

        assertThat(profile.isPreferredUsernameClaimAccepted(), is(false));
        assertThat(profile.isGroupsClaimMappedToGroupName(), is(true));
    }

    @Test
    public void shouldCreateWithAcceptUsernameGroups() {
        when(factoryInvoked().withArg("accept", "username,groups"));

        assertThat(profile.isPreferredUsernameClaimAccepted(), is(true));
        assertThat(profile.isGroupsClaimMappedToGroupName(), is(true));
    }

    @Test
    public void shouldCreateWithAcceptGroupsUsername() {
        when(factoryInvoked().withArg("accept", "groups,username"));

        assertThat(profile.isPreferredUsernameClaimAccepted(), is(true));
        assertThat(profile.isGroupsClaimMappedToGroupName(), is(true));
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

        public OidcProfile create() {
            return factory.create(arguments);
        }
    }
}