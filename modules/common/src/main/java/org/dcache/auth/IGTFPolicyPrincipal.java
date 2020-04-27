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

import java.io.Serializable;
import java.security.Principal;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * An IGTFPolicyPrincipal represents that the user has authenticated with
 * an X.509 credential issued by a Certificate Authority that satisfies
 * some IGTF policy.  These policies are define in policy-*.info files.
 * @since 3.1
 */
public class IGTFPolicyPrincipal implements Principal
{
    private final String _name;

    public IGTFPolicyPrincipal(String name)
    {
        checkArgument(!name.isEmpty());
        _name = name;
    }

    @Override
    public String getName()
    {
        return _name;
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

        if (!(other instanceof IGTFPolicyPrincipal)) {
            return false;
        }

        return ((IGTFPolicyPrincipal)other)._name.equals(_name);
    }

    @Override
    public String toString()
    {
        return "IGTFPolicy[" + _name + "]";
    }
}
