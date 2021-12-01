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
import diskCacheV111.util.FsPath;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.dcache.auth.ExemptFromNamespaceChecks;
import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.MultiTargetedRestriction;
import org.dcache.auth.attributes.Restriction;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.oidc.ProfileResult;

import static java.util.Objects.requireNonNull;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * This class provides a basis for AuthZ profiles that use the scope claim.
 */
abstract class ScopeBasedAuthzProfile extends BaseProfile {

    private static final Principal EXEMPT_FROM_NAMESPACE = new ExemptFromNamespaceChecks();

    private final FsPath prefix;

    protected ScopeBasedAuthzProfile(FsPath prefix) {
        this.prefix = requireNonNull(prefix);
    }

    @VisibleForTesting
    public FsPath getPrefix() {
        return prefix;
    }

    @Override
    public ProfileResult processClaims(IdentityProvider idp, Map<String,JsonNode> claims)
            throws AuthenticationException {

        ProfileResult result = super.processClaims(idp, claims);

        var node = claims.get("scope");
        checkAuthentication(node != null, "Missing 'scope' claim");
        checkAuthentication(node.isTextual(), "'scope' claim has wrong type: %s",
                node.getNodeType());

        String scopeClaim = node.asText();
        List<AuthorisationSupplier> authorisationStatements = parseScope(scopeClaim);

        if (!authorisationStatements.isEmpty()) {
            var newRestriction = buildRestriction(authorisationStatements);
            var newPrincipals = Collections.singleton(EXEMPT_FROM_NAMESPACE);
            result = result.withPrincipals(newPrincipals).withRestriction(newRestriction);
        }

        return result;
    }

    abstract protected List<AuthorisationSupplier> parseScope(String scope)
            throws AuthenticationException;

    private Restriction buildRestriction(List<AuthorisationSupplier> scopes) {
        Map<FsPath, MultiTargetedRestriction.Authorisation> authorisations = new HashMap<>();

        scopes.stream()
              .map(s -> s.authorisation(prefix))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .forEach(a -> {
                  FsPath path = a.getPath();
                  MultiTargetedRestriction.Authorisation existing = authorisations.get(path);
                  if (existing != null) {
                      Collection<Activity> combined = EnumSet.copyOf(existing.getActivity());
                      combined.addAll(a.getActivity());
                      a = new MultiTargetedRestriction.Authorisation(combined, path);
                  }
                  authorisations.put(path, a);
              });

        return new MultiTargetedRestriction(authorisations.values());
    }
}
