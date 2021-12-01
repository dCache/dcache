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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.EntitlementPrincipal;
import org.dcache.auth.FullNamePrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoA;
import org.dcache.auth.LoAPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.oidc.ProfileResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The OIDC profile handles claims that identify a person.
 */
public class OidcProfile extends BaseProfile {
    private static final Logger LOGGER = LoggerFactory.getLogger(OidcProfile.class);

    /**
     * A mapping from "eduperson_assurance" claim to the corresponding LoA. The details are
     * available in
     * <a href="https://docs.google.com/document/d/1b-Mlet3Lq7qKLEf1BnHJ4nL1fq-vMe7fzpXyrq2wp08/edit">REFEDs
     * OIDCre</a> and in various AARC policies,
     * <a href="https://aarc-project.eu/guidelines/aarc-g021/">AARC-G021</a> and
     * <a href="https://aarc-project.eu/guidelines/aarc-g041/">AARC-G041</a> in
     * particular.
     */
    private static final Map<String, LoA> EDUPERSON_ASSURANCE = ImmutableMap.<String, LoA>builder()
          // REFEDS RAF policies
          .put("https://refeds.org/assurance/ID/unique", LoA.REFEDS_ID_UNIQUE)
          .put("https://refeds.org/assurance/ID/eppn-unique-no-reassign",
                LoA.REFEDS_ID_EPPN_UNIQUE_NO_REASSIGN)
          .put("https://refeds.org/assurance/ID/eppn-unique-reassign-1y",
                LoA.REFEDS_ID_EPPN_UNIQUE_REASSIGN_1Y)
          .put("https://refeds.org/assurance/IAP/low", LoA.REFEDS_IAP_LOW)
          .put("https://refeds.org/assurance/IAP/medium", LoA.REFEDS_IAP_MEDIUM)
          .put("https://refeds.org/assurance/IAP/high", LoA.REFEDS_IAP_HIGH)
          .put("https://refeds.org/assurance/IAP/local-enterprise", LoA.REFEDS_IAP_LOCAL_ENTERPRISE)
          .put("https://refeds.org/assurance/ATP/ePA-1m", LoA.REFEDS_ATP_1M)
          .put("https://refeds.org/assurance/ATP/ePA-1d", LoA.REFEDS_ATP_1D)
          .put("https://refeds.org/assurance/profile/cappuccino", LoA.REFEDS_PROFILE_CAPPUCCINO)
          .put("https://refeds.org/assurance/profile/espresso", LoA.REFEDS_PROFILE_ESPRESSO)

          // IGTF policies  see https://www.igtf.net/ap/authn-assurance/
          .put("https://igtf.net/ap/authn-assurance/aspen", LoA.IGTF_LOA_ASPEN)
          .put("https://igtf.net/ap/authn-assurance/birch", LoA.IGTF_LOA_BIRCH)
          .put("https://igtf.net/ap/authn-assurance/cedar", LoA.IGTF_LOA_CEDAR)
          .put("https://igtf.net/ap/authn-assurance/dogwood", LoA.IGTF_LOA_DOGWOOD)

          // AARC policies see https://aarc-project.eu/guidelines/#policy
          .put("https://aarc-project.eu/policy/authn-assurance/assam", LoA.AARC_PROFILE_ASSAM)

          // EGI policies see https://wiki.egi.eu/wiki/AAI_guide_for_SPs#Level_of_Assurance
          .put("https://aai.egi.eu/LoA#Low", LoA.EGI_LOW)
          .put("https://aai.egi.eu/LoA#Substantial", LoA.EGI_SUBSTANTIAL)
          .put("https://aai.egi.eu/LoA#High", LoA.EGI_HIGH)
          .build();

    private final boolean acceptPreferredUsername;
    private final boolean acceptGroupsAsGroupNames;
    private final Function<String, Principal> toGroupPrincipal;

    public OidcProfile(boolean acceptPreferredUsername, boolean acceptGroups) {
        this.acceptPreferredUsername = acceptPreferredUsername;
        acceptGroupsAsGroupNames = acceptGroups;
        toGroupPrincipal = acceptGroups
              ? OidcProfile::toGroupName
              : OpenIdGroupPrincipal::new;
    }

    @VisibleForTesting
    public boolean isPreferredUsernameClaimAccepted() {
        return acceptPreferredUsername;
    }

    @VisibleForTesting
    public boolean isGroupsClaimMappedToGroupName() {
        return acceptGroupsAsGroupNames;
    }

    @Override
    public ProfileResult processClaims(IdentityProvider ip, Map<String, JsonNode> claims)
            throws AuthenticationException {
        ProfileResult result = super.processClaims(ip, claims);
        var extraPrincipals = extraPrincipals(claims);
        return result.withPrincipals(extraPrincipals);
    }

    private Set<Principal> extraPrincipals(Map<String,JsonNode> claims) {
        var extraPrincipals = new HashSet<Principal>();
        addNames(claims, extraPrincipals);
        addEmail(claims, extraPrincipals);
        addGroups(claims, extraPrincipals);
        addWlcgGroups(claims, extraPrincipals);
        addLoAs(claims, extraPrincipals);
        addEntitlements(claims, extraPrincipals);
        if (acceptPreferredUsername) {
            addUsername(claims, extraPrincipals);
        }
        return extraPrincipals;
    }


    private static GroupNamePrincipal toGroupName(String id) {
        /**
         * REVISIT: The group id (as supplied by the OP) may be hierarchical;
         * e.g. "/foo/bar".  For top-level groups (e.g., "/foo") the mapping
         * that removes the initial slash seems reasonable ("/foo" --> "foo");
         * however, how should this be handled more generally?
         * Mapping "/foo/bar" --> foo_bar is one option.  Should this be
         * configurable?
         */
        String name = id.startsWith("/") ? id.substring(1) : id;
        return new GroupNamePrincipal(name);
    }

    /**
     * Parse group-membership information, as described in "WLCG Common JWT Profiles" v1.0.  For
     * details, see: https://zenodo.org/record/3460258#.YVGMLyXRaV4
     * <p>
     * Here is an example:
     * <pre>
     * "wlcg.groups": [
     *     "/dteam/VO-Admin",
     *     "/dteam",
     *     "/dteam/itcms"
     * ],
     * </pre>
     *
     * REVISIT: should this be supported in the 'oidc' profile?  The semantics of this claim are
     * defined in the WLCG AuthZ JWT profile document, which is supported by a different profile.
     *
     * @param userInfo   The JSON node describing the user.
     * @param principals The set of principals into which any group information is to be added.
     */
    private void addWlcgGroups(Map<String,JsonNode> claims, Set<Principal> principals) {
        if (!claims.containsKey("wlcg.groups")) {
            return;
        }

        JsonNode groups = claims.get("wlcg.groups");
        if (!groups.isArray()) {
            LOGGER.debug("Ignoring malformed \"wlcg.groups\": not an array");
            return;
        }

        for (JsonNode group : groups) {
            if (!group.isTextual()) {
                LOGGER.debug("Ignoring malformed \"wlcg.groups\" value: {}", group);
                continue;
            }
            var groupName = group.asText();
            var principal = new OpenIdGroupPrincipal(groupName);
            principals.add(principal);
        }
    }

    private void addLoAs(Map<String,JsonNode> claims, Set<Principal> principals) {
        if (claims.containsKey("eduperson_assurance") && claims.get("eduperson_assurance").isArray()) {
            StreamSupport.stream(claims.get("eduperson_assurance").spliterator(), false)
                  .map(JsonNode::asText)
                  .map(EDUPERSON_ASSURANCE::get)
                  .filter(Objects::nonNull)
                  // FIXME we need to know when to accept REFEDS_IAP_LOCAL_ENTERPRISE.
                  .filter(l -> l != LoA.REFEDS_IAP_LOCAL_ENTERPRISE)
                  .map(LoAPrincipal::new)
                  .forEach(principals::add);
        }
    }

    private void addEmail(Map<String,JsonNode> claims, Set<Principal> principals) {
        if (claims.containsKey("email")) {
            principals.add(new EmailAddressPrincipal(claims.get("email").asText()));
        }
    }

    private void addNames(Map<String,JsonNode> claims, Set<Principal> principals) {
        JsonNode givenName = claims.get("given_name");
        JsonNode familyName = claims.get("family_name");
        JsonNode fullName = claims.get("name");

        if (fullName != null && !fullName.asText().isEmpty()) {
            principals.add(new FullNamePrincipal(fullName.asText()));
        } else if (givenName != null && !givenName.asText().isEmpty()
              && familyName != null && !familyName.asText().isEmpty()) {
            principals.add(new FullNamePrincipal(givenName.asText(), familyName.asText()));
        }
    }

    /**
     * Add Entitlement principals from mapped eduPersonEntitlement SAML assertions. For details of
     * mapping between SAML assertions and OIDC claims see https://wiki.refeds.org/display/CON/Consultation%3A+SAML2+and+OIDC+Mappings
     */
    private void addEntitlements(Map<String,JsonNode> claims, Set<Principal> principals) {
        JsonNode value = claims.get("eduperson_entitlement");
        if (value == null) {
            return;
        }

        if (value.isArray()) {
            StreamSupport.stream(value.spliterator(), false)
                  .map(JsonNode::asText)
                  .forEach(v -> addEntitlement(principals, v));
        } else if (value.isTextual()) {
            addEntitlement(principals, value.asText());
        }
    }


    private void addGroups(Map<String,JsonNode> claims, Set<Principal> principals) {
        if (claims.containsKey("groups") && claims.get("groups").isArray()) {
            for (JsonNode group : claims.get("groups")) {
                String groupName = group.asText();
                var principal = toGroupPrincipal.apply(groupName);
                principals.add(principal);
            }
        }
    }

    private void addUsername(Map<String,JsonNode> claims, Set<Principal> principals) {
        JsonNode value = claims.get("preferred_username");
        if (value != null && value.isTextual()) {
            principals.add(new UserNamePrincipal(value.asText()));
        }
    }

    private void addEntitlement(Set<Principal> principals, String value) {
        try {
            principals.add(new EntitlementPrincipal(value));
        } catch (URISyntaxException e) {
            LOGGER.debug("Rejecting bad eduperson_entitlement value \"{}\": {}",
                  value, e.getMessage());
        }
    }
}