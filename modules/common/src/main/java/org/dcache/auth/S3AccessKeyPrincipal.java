/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.CharMatcher;
import java.io.Serializable;
import java.security.Principal;

/**
 * The access key ID of an S3 client whose request signature the S3 door has verified.  gPlazma
 * maps it to a user like any other authenticated identity.
 * <p>
 * This principal is not part of the login result: older dCache versions could not deserialise it.
 */
@AuthenticationInput
public class S3AccessKeyPrincipal implements Principal, Serializable {

    private static final long serialVersionUID = 1L;

    private final String _accessKey;

    public S3AccessKeyPrincipal(String accessKey) {
        checkArgument(!accessKey.isEmpty(), "S3 access key can't be an empty string");
        checkArgument(CharMatcher.whitespace().matchesNoneOf(accessKey),
              "S3 access key can't contain whitespace");
        _accessKey = accessKey;
    }

    @Override
    public String getName() {
        return _accessKey;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof S3AccessKeyPrincipal)) {
            return false;
        }
        return _accessKey.equals(((S3AccessKeyPrincipal) other)._accessKey);
    }

    @Override
    public int hashCode() {
        return _accessKey.hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '[' + getName() + ']';
    }
}
