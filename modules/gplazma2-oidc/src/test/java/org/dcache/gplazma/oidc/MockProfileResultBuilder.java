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
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.dcache.auth.attributes.Restriction;
import org.mockito.BDDMockito;

import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.mock;

/** A fluent class for building a (mock) ProfileResult instance. */
public class MockProfileResultBuilder {
    private final ProfileResult result = mock(ProfileResult.class);
    private Restriction restriction;
    private Set<Principal> principals;

    public static MockProfileResultBuilder aProfileResult() {
        return new MockProfileResultBuilder();
    }

    public MockProfileResultBuilder withRestriction(Restriction restriction) {
        this.restriction = restriction;
        return this;
    }

    public MockProfileResultBuilder withNoRestriction() {
        restriction = null;
        return this;
    }

    public MockProfileResultBuilder withNoPrincipals() {
        principals = Collections.emptySet();
        return this;
    }

    public MockProfileResultBuilder withPrincipals(Set<Principal> principals) {
        this.principals = principals;
        return this;
    }

    public ProfileResult build() {
        var maybeRestriction = Optional.ofNullable(restriction);
        BDDMockito.given(result.getRestriction()).willReturn(maybeRestriction);
        BDDMockito.given(result.getPrincipals()).willReturn(requireNonNull(principals));
        return result;
    }
}
