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
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectProfileWithRelativePrefix() {
        when(factoryInvoked().withArg("prefix", "relative/path"));
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