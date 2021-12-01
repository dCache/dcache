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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.oidc.MockIdentityProviderBuilder;
import org.dcache.gplazma.oidc.Profile;
import org.dcache.gplazma.oidc.ProfileResult;

import static java.util.Objects.requireNonNull;


public class ProfileInvocationBuilder {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String,JsonNode> claims = new HashMap<>();
    private final Profile profile;

    private IdentityProvider idp;

    ProfileInvocationBuilder(Profile profile) {
        this.profile = requireNonNull(profile);
    }

    public ProfileInvocationBuilder withStringClaim(String id, String value) {
        return this.withClaim(id, "\"" + value + "\"");
    }

    public ProfileInvocationBuilder withClaim(String id, String json) {
        try {
            claims.put(id, mapper.readTree(json));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Bad claim value " + json + ": " + e);
        }
        return this;
    }

    public ProfileInvocationBuilder withIdP(MockIdentityProviderBuilder builder) {
        idp = builder.build();
        return this;
    }

    public ProfileResult invoke() throws AuthenticationException {
        requireNonNull(idp, "IdentityProfiler not configured");
        return profile.processClaims(idp, claims);
    }
}
