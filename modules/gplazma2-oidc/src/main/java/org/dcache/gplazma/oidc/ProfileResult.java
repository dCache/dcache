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

import java.security.Principal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;

import static java.util.Objects.requireNonNull;

/**
 * A ProfileResult class provides the outcome of a Profile processing a set of claims.
 */
public class ProfileResult {
    private final Set<Principal> principals;
    private final Optional<Restriction> restriction;

    public ProfileResult(Set<Principal> principals) {
        this(principals, null);
    }

    public ProfileResult(Set<Principal> principals, @Nullable Restriction restriction) {
        this.principals = requireNonNull(principals);
        this.restriction = Optional.ofNullable(restriction);
    }

    public Set<Principal> getPrincipals() {
        return principals;
    }

    public Optional<Restriction> getRestriction() {
        return restriction;
    }

    public ProfileResult withPrincipals(Collection<Principal> additionalPrincipals) {
        if (additionalPrincipals.isEmpty()) {
            return this;
        }

        var newPrincipals = new HashSet<Principal>();
        newPrincipals.addAll(principals);
        newPrincipals.addAll(additionalPrincipals);
        return new ProfileResult(newPrincipals, restriction.orElse(null));
    }

    public ProfileResult withRestriction(Restriction additionalRestriction) {
        Restriction newRestriction = restriction.map(r -> Restrictions.concat(r, additionalRestriction))
                .orElse(additionalRestriction);
        return new ProfileResult(principals, newRestriction);
    }
}
