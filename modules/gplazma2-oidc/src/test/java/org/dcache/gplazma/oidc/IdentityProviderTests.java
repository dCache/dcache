/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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

import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class IdentityProviderTests
{

    @Test(expected = NullPointerException.class)
    public void shouldFailWithNullName() throws Exception
    {
        IdentityProvider ignored = new IdentityProvider(null, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithEmptyName() throws Exception
    {
        IdentityProvider ignored = new IdentityProvider("", "");
    }

    @Test(expected = NullPointerException.class)
    public void shouldFailWithNullDescription() throws Exception
    {
        IdentityProvider ignored = new IdentityProvider("null-provider", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithEmptyDescription() throws Exception
    {
        IdentityProvider ignored = new IdentityProvider("empty-provider", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithInvalidUri() throws Exception
    {
        IdentityProvider ignored = new IdentityProvider("bad-url", "0:0");
    }

    @Test
    public void shouldParseProviderWithTrailingSlash() throws Exception
    {
        IdentityProvider google = new IdentityProvider("google", "https://accounts.google.com/");

        assertThat(google.getName(), is(equalTo("google")));
        assertThat(google.getIssuerEndpoint().toString(), is(equalTo("https://accounts.google.com/")));
        assertThat(google.getConfigurationEndpoint().toString(), is(equalTo("https://accounts.google.com/.well-known/openid-configuration")));
    }

    @Test
    public void shouldParseProviderWithoutTrailingSlash() throws Exception
    {
        IdentityProvider google = new IdentityProvider("google", "https://accounts.google.com");

        assertThat(google.getName(), is(equalTo("google")));
        assertThat(google.getIssuerEndpoint().toString(), is(equalTo("https://accounts.google.com")));
        assertThat(google.getConfigurationEndpoint().toString(), is(equalTo("https://accounts.google.com/.well-known/openid-configuration")));
    }

    @Test
    public void shouldParseProviderWithPathWithoutTrailingSlash() throws Exception
    {
        IdentityProvider google = new IdentityProvider("unity", "https://unity.helmholtz-data-federation.de/oauth2");

        assertThat(google.getName(), is(equalTo("unity")));
        assertThat(google.getIssuerEndpoint().toString(), is(equalTo("https://unity.helmholtz-data-federation.de/oauth2")));
        assertThat(google.getConfigurationEndpoint().toString(), is(equalTo("https://unity.helmholtz-data-federation.de/oauth2/.well-known/openid-configuration")));
    }

    @Test
    public void shouldParseProviderWithPathWithTrailingSlash() throws Exception
    {
        IdentityProvider google = new IdentityProvider("unity", "https://unity.helmholtz-data-federation.de/oauth2/");

        assertThat(google.getName(), is(equalTo("unity")));
        assertThat(google.getIssuerEndpoint().toString(), is(equalTo("https://unity.helmholtz-data-federation.de/oauth2/")));
        assertThat(google.getConfigurationEndpoint().toString(), is(equalTo("https://unity.helmholtz-data-federation.de/oauth2/.well-known/openid-configuration")));
    }
}
