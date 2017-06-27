/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth.attributes;

import java.io.Serializable;

/**
 * A role that the user may assert but, for this login, chose not to.  This
 * LoginAttribute allows a door to provide the user with a list of roles that
 * may be asserted.
 */
public class UnassertedRole implements LoginAttribute, Serializable
{
    private static final long serialVersionUID = 1L;

    private final String _name;

    public UnassertedRole(String name)
    {
        _name = name;
    }

    public String getRole()
    {
        return _name;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof UnassertedRole)) {
            return false;
        }
        UnassertedRole other = (UnassertedRole) obj;
        return _name.equals(other._name);
    }

    @Override
    public int hashCode()
    {
        return _name.hashCode();
    }

    @Override
    public String toString()
    {
        return "UnassertedRole[" + _name + ']';
    }
}
