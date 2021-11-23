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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.net.URI;


/**
 * An OpenID-Connect Identity Provider.  An identity provider is a service that the admin has chosen
 * to trust in authenticating users via the OpenID-Connect protocol.  This class holds the
 * configuration information about a provider.
 * <p>
 * Each OIDC identity provider is assigned a name by the admin, which is used typically when
 * referring to the service in log messages.
 */
public class IdentityProvider {

    private final String name;
    private final URI issuer;
    private final URI configuration;
    private final Profile profile;

    public IdentityProvider(String name, URI endpoint, Profile profile) {
        checkArgument(!name.isEmpty(), "Empty name not allowed");
        this.name = name;
        this.issuer = requireNonNull(endpoint);
        checkArgument(endpoint.isAbsolute(), "URL is not absolute");
        this.profile = requireNonNull(profile);

        configuration = issuer.resolve(
              withTrailingSlash(issuer.getPath()) + ".well-known/openid-configuration");
    }

    private static String withTrailingSlash(String path) {
        return path.endsWith("/") ? path : (path + "/");
    }

    public String getName() {
        return name;
    }

    public URI getIssuerEndpoint() {
        return issuer;
    }

    public Profile getProfile() {
        return profile;
    }

    /**
     * The URI of the discovery endpoint.  This URL is defined in "OpenID Connect Discovery" section
     * 4:
     * <p>
     * https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfig
     */
    public URI getConfigurationEndpoint() {
        return configuration;
    }

    @Override
    public int hashCode() {
        return name.hashCode() ^ issuer.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof IdentityProvider)) {
            return false;
        }

        IdentityProvider otherIP = (IdentityProvider) other;
        return name.equals(otherIP.name) && issuer.equals(otherIP.issuer);
    }

    @Override
    public String toString() {
        return name + "[" + issuer.toASCIIString() + "]";
    }
}
