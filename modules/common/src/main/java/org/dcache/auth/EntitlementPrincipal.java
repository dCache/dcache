/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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

import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;

/**
 * A Principal that represents an eduPersonEntitlement asserted value.
 * The eduPerson set of assertions are defined by REFEDS.  For further details
 * see:
 * https://wiki.refeds.org/display/STAN/eduPerson+2020-01#eduPerson202001-eduPersonEntitlement
 */
public class EntitlementPrincipal implements Principal
{
    private final String name;

    public EntitlementPrincipal(String value) throws URISyntaxException
    {
        URI uri = new URI(value);

        // REVISIT validate URI further?

        this.name = uri.toASCIIString();
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        EntitlementPrincipal otherEntitlement = (EntitlementPrincipal) other;
        return otherEntitlement.name.equals(name);
    }

    @Override
    public String toString()
    {
        return "Entitlement[" + name + "]";
    }
}
