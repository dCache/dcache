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
package org.dcache.gplazma.oidc;

import java.security.Principal;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import org.mockito.ArgumentMatchers;
import org.mockito.BDDMockito;

import static org.mockito.Mockito.mock;

/** A fluent class for building a mock Profile. */
public class MockProfileBuilder {
    private final Profile profile = mock(Profile.class);

    public static MockProfileBuilder aProfile() {
        return new MockProfileBuilder();
    }

    public MockProfileBuilder thatReturns(Set<Principal> principals) {
        try {
            BDDMockito.given(profile.processClaims(ArgumentMatchers.any(), ArgumentMatchers.any())).willReturn(principals);
        } catch (AuthenticationException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public MockProfileBuilder thatThrows(AuthenticationException e) throws AuthenticationException {
        BDDMockito.given(profile.processClaims(ArgumentMatchers.any(), ArgumentMatchers.any())).willThrow(e);
        return this;
    }

    public MockProfileBuilder thatFailsTestIfCalled() {
        try {
            BDDMockito.given(profile.processClaims(ArgumentMatchers.any(), ArgumentMatchers.any()))
                    .willThrow(new AssertionError("Profile#processClaims called"));
        } catch (AuthenticationException e) {
            throw new RuntimeException("Impossible exception caught", e);
        }
        return this;
    }

    public Profile build() {
        return profile;
    }
}
