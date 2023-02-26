/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 - 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.oidc;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class IdentityProviderTests {
    private static final Profile IGNORE_ALL = (i,c) -> Collections.emptySet();
    private static final List<String> NO_SUPPRESSION = List.of();

    @Test(expected = NullPointerException.class)
    public void shouldFailWithNullName() throws Exception {
        IdentityProvider ignored = new IdentityProvider(null, URI.create("http://example.org/"), IGNORE_ALL, NO_SUPPRESSION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithEmptyName() throws Exception {
        IdentityProvider ignored = new IdentityProvider("", URI.create("http://example.org/"), IGNORE_ALL, NO_SUPPRESSION);
    }

    @Test(expected = NullPointerException.class)
    public void shouldFailWithNullUri() throws Exception {
        IdentityProvider ignored = new IdentityProvider("null-provider", null, IGNORE_ALL, NO_SUPPRESSION);
    }

    @Test(expected = NullPointerException.class)
    public void shouldFailWithNullProfile() throws Exception {
        IdentityProvider ignored = new IdentityProvider("null-profile", URI.create("http://example.org/"), null, NO_SUPPRESSION);
    }

    @Test
    public void shouldParseProviderWithTrailingSlash() throws Exception {
        IdentityProvider google = new IdentityProvider("GOOGLE", URI.create("https://accounts.google.com/"),
                IGNORE_ALL, NO_SUPPRESSION);

        assertThat(google.getName(), is(equalTo("GOOGLE")));
        assertThat(google.getIssuerEndpoint().toString(),
              is(equalTo("https://accounts.google.com/")));
        assertThat(google.getConfigurationEndpoint().toString(),
              is(equalTo("https://accounts.google.com/.well-known/openid-configuration")));
        assertThat(google.getProfile(), is(sameInstance(IGNORE_ALL)));
    }

    @Test
    public void shouldParseProviderWithoutTrailingSlash() throws Exception {
        IdentityProvider google = new IdentityProvider("GOOGLE", URI.create("https://accounts.google.com"),
                IGNORE_ALL, NO_SUPPRESSION);

        assertThat(google.getName(), is(equalTo("GOOGLE")));
        assertThat(google.getIssuerEndpoint().toString(),
              is(equalTo("https://accounts.google.com")));
        assertThat(google.getConfigurationEndpoint().toString(),
              is(equalTo("https://accounts.google.com/.well-known/openid-configuration")));
        assertThat(google.getProfile(), is(sameInstance(IGNORE_ALL)));
    }

    @Test
    public void shouldParseProviderWithPathWithoutTrailingSlash() throws Exception {
        IdentityProvider unity = new IdentityProvider("UNITY",
              URI.create("https://unity.helmholtz-data-federation.de/oauth2"), IGNORE_ALL, NO_SUPPRESSION);

        assertThat(unity.getName(), is(equalTo("UNITY")));
        assertThat(unity.getIssuerEndpoint().toString(),
              is(equalTo("https://unity.helmholtz-data-federation.de/oauth2")));
        assertThat(unity.getConfigurationEndpoint().toString(), is(equalTo(
              "https://unity.helmholtz-data-federation.de/oauth2/.well-known/openid-configuration")));
        assertThat(unity.getProfile(), is(sameInstance(IGNORE_ALL)));
    }

    @Test
    public void shouldParseProviderWithPathWithTrailingSlash() throws Exception {
        IdentityProvider unity = new IdentityProvider("UNITY",
              URI.create("https://unity.helmholtz-data-federation.de/oauth2/"), IGNORE_ALL, NO_SUPPRESSION);

        assertThat(unity.getName(), is(equalTo("UNITY")));
        assertThat(unity.getIssuerEndpoint().toString(),
              is(equalTo("https://unity.helmholtz-data-federation.de/oauth2/")));
        assertThat(unity.getConfigurationEndpoint().toString(), is(equalTo(
              "https://unity.helmholtz-data-federation.de/oauth2/.well-known/openid-configuration")));
        assertThat(unity.getProfile(), is(sameInstance(IGNORE_ALL)));
    }
}
