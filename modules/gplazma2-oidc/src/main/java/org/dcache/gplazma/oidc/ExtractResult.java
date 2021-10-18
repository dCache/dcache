/* dCache - http://www.dcache.org/
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
package org.dcache.gplazma.oidc;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * This class encapsulates the information obtained from successfully processing a token.
 * @see TokenProcessor
 */
public class ExtractResult {
    private final Map<String,JsonNode> claims;
    private final IdentityProvider ip;

    public ExtractResult(IdentityProvider ip, Map<String,JsonNode> claims) {
        this.claims = requireNonNull(claims);
        this.ip = requireNonNull(ip);
    }

    /**
     * The set of claims associated with this token.  Claims are key-value pairs, where the key is
     * a simple string and the value is some valid JSON value.  Certain keys are well-defined, in
     * that their meaning are documented as part of some standard.  Others are more ad-hoc: either
     * community-drive conventions, implementation specific or even instance-specific values.
     * @return The claims obtains from this token.
     */
    @Nonnull
    public Map<String,JsonNode> claims() {
        return claims;
    }

    /**
     * The OAuth Provider that asserted these claims.   This is usually the agent that issued the
     * token, but this is not a requirement.  Since many of these claims are about a person's
     * identity, the OP is sometimes referred to as an Identity Provider, or "IdP" for short.
     * @return The agent that asserts the claims.
     */
    @Nonnull
    public IdentityProvider idp() {
        return ip;
    }

    @Override
    public int hashCode() {
        return claims.hashCode() ^ ip.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof ExtractResult)) {
            return false;
        }
        ExtractResult otherResult = (ExtractResult)other;

        return claims.equals(otherResult.claims) && ip.equals(otherResult.ip);
    }

    @Override
    public String toString() {
        return "{IP:" + ip + ", claims:" + claims + "}";
    }
}
