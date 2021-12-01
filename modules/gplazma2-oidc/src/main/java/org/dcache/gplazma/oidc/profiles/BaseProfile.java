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
package org.dcache.gplazma.oidc.profiles;

import com.fasterxml.jackson.databind.JsonNode;
import java.security.Principal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.dcache.auth.JwtJtiPrincipal;
import org.dcache.auth.JwtSubPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.oidc.Profile;
import org.dcache.gplazma.oidc.ProfileResult;

/**
 * This class provides a common behaviour for Profiles.  In particular, it adds Principals that
 * represent any {@literal sub} and {@literal jti} claims.
 * <p>
 * It is recommended (but not required) that Profile classes extend this class to avoid code
 * duplication.
 */
public class BaseProfile implements Profile {

    @Override
    public ProfileResult processClaims(IdentityProvider idp, Map<String, JsonNode> claims)
            throws AuthenticationException {
        Set<Principal> principals = new HashSet<>();

        addSub(idp, claims, principals);
        addJti(idp, claims, principals);

        return new ProfileResult(principals);
    }

    private void addSub(IdentityProvider idp, Map<String,JsonNode> claims,
            Set<Principal> principals) {
        var node = claims.get("sub");

        if (node != null && node.isTextual()) {
            String claimValue = node.asText();
            principals.add(new OidcSubjectPrincipal(claimValue, idp.getName()));

            // REVISIT: the JwtSubPrincipal is only included for backwards compatibility.  It is
            // not used by dCache and should (very likely) be removed.
            principals.add(new JwtSubPrincipal(idp.getName(), claimValue));
        }
    }

    private void addJti(IdentityProvider idp, Map<String,JsonNode> claims,
          Set<Principal> principals) {
        var node = claims.get("jti");

        if (node != null && node.isTextual()) {
            var value = node.asText();
            principals.add(new JwtJtiPrincipal(idp.getName(), value));
        }
    }
}
