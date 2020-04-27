/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
 * The principal that defines what kind of entity has authenticated.
 * @since 2.14
 */
public class EntityDefinitionPrincipal implements Principal, Serializable
{
    private static final long serialVersionUID = 1L;

    private final EntityDefinition _definition;

    public EntityDefinitionPrincipal(EntityDefinition definition)
    {
        _definition = definition;
    }

    public EntityDefinition getDefinition()
    {
        return _definition;
    }

    @Override
    public String getName()
    {
        return _definition.getName();
    }

    @Override
    public int hashCode()
    {
        return _definition.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (!(other instanceof EntityDefinitionPrincipal)) {
            return false;
        }

        return _definition == ((EntityDefinitionPrincipal)other)._definition;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '[' + getName() + ']';
    }
}
