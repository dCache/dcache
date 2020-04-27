/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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

import java.io.Serializable;
import java.security.Principal;

/**
 * Any principal that where the value is unique only within a specific OAuth2
 * provider.
 * @since 5.1
 */
public abstract class OpScopedPrincipal implements Principal, Serializable
{
    private final String name;

    public OpScopedPrincipal(String op, String sub)
    {
        name = op + ":" + sub;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '[' + getName() + ']';
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }

        return this.getClass().equals(other.getClass())
                && ((Principal)other).getName().equals(name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }
}
