/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth;

import java.net.URI;
import static java.util.Objects.requireNonNull;

import java.security.Principal;

/**
 * A Principal that describes which OP asserted the users identity.  The value is the short-hand
 * name for the OP.
 */
public class OAuthProviderPrincipal implements Principal {

    private final String name;
    private final URI issuer;

    public OAuthProviderPrincipal(String name, URI issuer) {
        this.name = requireNonNull(name);
        this.issuer = requireNonNull(issuer);
    }

    @Override
    public String getName() {
        return name;
    }

    public URI getIssuer() {
        return issuer;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof OAuthProviderPrincipal
              && ((OAuthProviderPrincipal) other).name.equals(name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return "OP[" + name + "]";
    }
}
