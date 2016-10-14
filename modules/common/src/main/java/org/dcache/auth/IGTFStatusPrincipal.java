/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

import java.security.Principal;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A principal that represents the user authenticated with a certificate
 * authority that has some specific IGTF status.  The status is defined in
 * that certificate authority's .info file.
 */
public class IGTFStatusPrincipal implements Principal
{
    private final String _name;
    private final boolean _isAccredited;

    public IGTFStatusPrincipal(String name, boolean accredited)
    {
        checkArgument(!name.isEmpty());
        _name = name;
        _isAccredited = accredited;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    public boolean isAccredited()
    {
        return _isAccredited;
    }

    @Override
    public int hashCode()
    {
        return _name.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (!(other instanceof IGTFStatusPrincipal)) {
            return false;
        }

        IGTFStatusPrincipal that = ((IGTFStatusPrincipal)other);

        return _name.equals(that._name) && _isAccredited == that._isAccredited;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("IGTFStatus[");
        if (_isAccredited) {
            sb.append('*');
        }
        sb.append(_name).append(']');
        return sb.toString();
    }
}
