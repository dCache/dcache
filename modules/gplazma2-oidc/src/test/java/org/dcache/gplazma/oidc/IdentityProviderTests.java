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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


public class IdentityProviderTests {
    private static final Profile IGNORE_ALL = (i,c) -> new ProfileResult(Collections.emptySet());

    @Test(expected = NullPointerException.class)
    public void shouldFailWithNullName() throws Exception {
        IdentityProvider ignored = new IdentityProvider(null, URI.create("http://example.org/"), IGNORE_ALL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithEmptyName() throws Exception {
        IdentityProvider ignored = new IdentityProvider("", URI.create("http://example.org/"), IGNORE_ALL);
    }

    @Test(expected = NullPointerException.class)
    public void shouldFailWithNullUri() throws Exception {
        IdentityProvider ignored = new IdentityProvider("null-provider", null, IGNORE_ALL);
    }

    @Test(expected = NullPointerException.class)
    public void shouldFailWithNullProfile() throws Exception {
        IdentityProvider ignored = new IdentityProvider("null-profile", URI.create("http://example.org/"), null);
    }

    @Test
    public void shouldEqualReflectively() throws Exception {
        IdentityProvider google = new IdentityProvider("GOOGLE", URI.create("https://accounts.google.com/"), IGNORE_ALL);

        assertTrue(google.equals(google));
    }

    @Test
    public void shouldEqualAnotherWithSameNameAndUrl() throws Exception {
        IdentityProvider google1 = new IdentityProvider("GOOGLE", URI.create("https://accounts.google.com/"), IGNORE_ALL);
        IdentityProvider google2 = new IdentityProvider("GOOGLE", URI.create("https://accounts.google.com/"), IGNORE_ALL);

        assertTrue(google1.hashCode() == google2.hashCode());
        assertTrue(google1.equals(google2));
    }

    @Test
    public void shouldNotEqualAnotherWithDifferentName() throws Exception {
        IdentityProvider google1 = new IdentityProvider("GOOGLE-1", URI.create("https://accounts.google.com/"), IGNORE_ALL);
        IdentityProvider google2 = new IdentityProvider("GOOGLE-2", URI.create("https://accounts.google.com/"), IGNORE_ALL);

        assertFalse(google1.equals(google2));
    }

    @Test
    public void shouldNotEqualAnotherWithDifferentUrl() throws Exception {
        IdentityProvider google = new IdentityProvider("MYIP", URI.create("https://accounts.google.com/"), IGNORE_ALL);
        IdentityProvider keycloak = new IdentityProvider("MYIP", URI.create("https://keycloak.desy.de/"), IGNORE_ALL);

        assertFalse(google.equals(keycloak));
    }

    @Test
    public void shouldNotEqualAnotherType() throws Exception {
        IdentityProvider google = new IdentityProvider("MYIP", URI.create("https://accounts.google.com/"), IGNORE_ALL);

        assertFalse(google.equals("MYIP"));
        assertFalse(google.equals("https://accounts.google.com/"));
        assertFalse(google.equals(URI.create("https://accounts.google.com/")));
    }

    @Test
    public void shouldReturnStringWithNameAndUrlWhenToStringCalled() throws Exception {
        IdentityProvider google = new IdentityProvider("GOOGLE", URI.create("https://accounts.google.com/"), IGNORE_ALL);

        assertThat(google.toString(), containsString("GOOGLE"));
        assertThat(google.toString(), containsString("https://accounts.google.com/"));
    }

    @Test
    public void shouldParseProviderWithTrailingSlash() throws Exception {
        IdentityProvider google = new IdentityProvider("GOOGLE", URI.create("https://accounts.google.com/"),
                IGNORE_ALL);

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
                IGNORE_ALL);

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
              URI.create("https://unity.helmholtz-data-federation.de/oauth2"), IGNORE_ALL);

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
              URI.create("https://unity.helmholtz-data-federation.de/oauth2/"), IGNORE_ALL);

        assertThat(unity.getName(), is(equalTo("UNITY")));
        assertThat(unity.getIssuerEndpoint().toString(),
              is(equalTo("https://unity.helmholtz-data-federation.de/oauth2/")));
        assertThat(unity.getConfigurationEndpoint().toString(), is(equalTo(
              "https://unity.helmholtz-data-federation.de/oauth2/.well-known/openid-configuration")));
        assertThat(unity.getProfile(), is(sameInstance(IGNORE_ALL)));
    }
}
