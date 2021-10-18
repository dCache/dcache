/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019-2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.oidc.userinfo;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.Map;
import org.dcache.gplazma.oidc.IdentityProvider;

import static java.util.Objects.requireNonNull;

/**
 * This class represents the result of a user-info request.
 */
public class LookupResult {

    private final Map<String,JsonNode> claims;
    private final String error;
    private final IdentityProvider ip;

    public static LookupResult error(IdentityProvider ip, String message) {
        return new LookupResult(ip, Collections.emptyMap(), requireNonNull(message));
    }

    public static LookupResult success(IdentityProvider ip, Map<String,JsonNode> claims) {
        return new LookupResult(ip, claims, null);
    }

    private LookupResult(IdentityProvider ip, Map<String,JsonNode> claims, String error) {
        this.claims = requireNonNull(claims);
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

    public Map<String,JsonNode> getClaims() {
        return claims;
    }
}
