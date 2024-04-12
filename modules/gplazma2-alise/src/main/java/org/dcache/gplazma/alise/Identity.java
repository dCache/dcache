/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2024 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.alise;

import com.google.common.base.Objects;
import java.net.URI;
import static java.util.Objects.requireNonNull;

/**
 * The identity that ALISE will look up.
 */
public class Identity {
    private final String sub;
    private final URI issuer;

    public Identity(URI issuer, String sub) {
        this.issuer = requireNonNull(issuer);
        this.sub = requireNonNull(sub);
    }

    public String sub() {
        return sub;
    }

    public URI issuer() {
        return issuer;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof Identity)) {
            return false;
        }

        Identity otherIdentity = (Identity)other;
        return otherIdentity.sub.equals(sub) && otherIdentity.issuer.equals(issuer);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(sub, issuer);
    }

    @Override
    public String toString() {
        return "{"+issuer.toASCIIString()+"}"+sub;
    }
}
