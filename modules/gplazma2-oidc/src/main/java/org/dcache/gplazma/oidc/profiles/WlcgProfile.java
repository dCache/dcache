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
import com.google.common.base.Splitter;
import diskCacheV111.util.FsPath;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.oidc.ProfileResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * A profile that processes the claims from an AuthZ-WG profile token.  The profile is described
 * in the document "WLCG Common JWT Profiles" v1.0, which is available here:
 *
 * https://zenodo.org/record/3460258#.YVGMLyXRaV4
 */
public class WlcgProfile extends ScopeBasedAuthzProfile {
    private static final Logger LOGGER = LoggerFactory.getLogger(WlcgProfile.class);

    public WlcgProfile(FsPath prefix) {
        super(prefix);
    }

    @Override
    public ProfileResult processClaims(IdentityProvider idp, Map<String,JsonNode> claims)
            throws AuthenticationException {
        checkProfileVersion(claims);
        ProfileResult result = super.processClaims(idp, claims);
        return result.withPrincipals(additionalPrincipals(claims));
    }

    private void checkProfileVersion(Map<String,JsonNode> claims) throws AuthenticationException {
        JsonNode ver = claims.get("wlcg.ver");
        checkAuthentication(ver != null, "wlcg.ver claim is missing");
        checkAuthentication(ver.isTextual(), "wlcg.ver claim not textural");
        checkAuthentication(ver.asText().equals("1.0"), "Unsupported version of WLCG profile \"%s\"",
                ver);
    }

    private List<Principal> additionalPrincipals(Map<String,JsonNode> claims) {
        return wlcgGroups(claims);
    }

    private List<Principal> wlcgGroups(Map<String,JsonNode> claims) {
        if (!claims.containsKey("wlcg.groups")) {
            return Collections.emptyList();
        }

        JsonNode groups = claims.get("wlcg.groups");
        if (!groups.isArray()) {
            LOGGER.debug("Ignoring malformed \"wlcg.groups\": not an array");
            return Collections.emptyList();
        }

        List<Principal> principals = new ArrayList<>();
        for (JsonNode group : groups) {
            if (!group.isTextual()) {
                LOGGER.debug("Ignoring malformed \"wlcg.groups\" value: {}", group);
                continue;
            }
            var groupName = group.asText();
            var principal = new OpenIdGroupPrincipal(groupName);
            principals.add(principal);
        }
        return principals;
    }

    @Override
    protected List<AuthorisationSupplier> parseScope(String claim) {
        return Splitter.on(' ').trimResults().splitToList(claim).stream()
                .filter(WlcgProfileScope::isWlcgProfileScope)
                .map(WlcgProfileScope::new)
                .collect(Collectors.toList());
    }
}
