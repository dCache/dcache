/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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

import static java.util.Objects.requireNonNull;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

/**
 * This class represents the result of a user-info request.
 */
public class LookupResult {

    private final Set<Principal> principals;
    private final String error;
    private final IdentityProvider ip;

    public static LookupResult error(IdentityProvider ip, String message) {
        return new LookupResult(ip, Collections.emptySet(), requireNonNull(message));
    }

    public static LookupResult success(IdentityProvider ip, Set<Principal> principals) {
        return new LookupResult(ip, principals, null);
    }

    private LookupResult(IdentityProvider ip, Set<Principal> principals, String error) {
        this.principals = requireNonNull(principals);
        this.ip = requireNonNull(ip);
        this.error = error;
    }

    public IdentityProvider getIdentityProvider() {
        return ip;
    }

    public boolean isSuccess() {
        return error == null;
    }

    public String getError() {
        return error;
    }

    public Set<Principal> getPrincipals() {
        return principals;
    }
}
