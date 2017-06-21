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
 * Some users have the ability to log into dCache so that dCache has behaves
 * differently, depending on exactly how they have logged in.  For example, a
 * user that is also a dCache admin can choose whether they wish to
 * authenticate as a normal dCache user, or as a user with admin privileges.
 * The presence of Role LoginAttribute captures these differences.
 * A user may have zero or more roles, with the presence of roles potentially
 * adjusting how dCache behaves to user requests.
 */
public class Role implements LoginAttribute, Serializable
{
    private static final long serialVersionUID = 1L;

    private final String _name;

    public Role(String name)
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
        if (!(obj instanceof Role)) {
            return false;
        }
        Role other = (Role) obj;
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
        return "Role[" + _name + ']';
    }
}
