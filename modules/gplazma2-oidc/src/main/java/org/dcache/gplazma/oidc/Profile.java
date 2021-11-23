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
import java.security.Principal;
import java.util.Map;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;

/**
 * A Profile provides a way of mapping the OIDC claims obtained from the OP into dCache-specific
 * information.  Profiles may also reject the token if the claims are somehow incompatible with
 * the profile's expectations.
 * @see ProfileFactory
 */
@FunctionalInterface
public interface Profile {
    /**
     * Handle the set of claims coming from the OP and return information for dCache.  The
     * information for dCache is in the form of a set of principals.
     * <p>
     * Although the exact nature of those principals is under the control of the Profile, there is
     * a contract between the OIDC plugin and the profile that the profile will either return at
     * least one principal or it will throw AuthenticationException.  The profile must not return
     * an empty set of principals.
     * @param idp The OP that supplied the claims
     * @param claims The claims coming from the OP
     * @return The set of principals to add.
     * @throws AuthenticationException if the login should fail.
     */
    Set<Principal> processClaims(IdentityProvider idp, Map<String,JsonNode> claims)
            throws AuthenticationException;
}
