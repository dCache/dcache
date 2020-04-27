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
package org.dcache.auth;

import java.io.Serializable;
import java.security.Principal;

import static java.util.Objects.requireNonNull;

/**
 * Identifies the Macaroon used to authorise the activity.
 * @since 3.2
 */
public class MacaroonPrincipal implements Principal, Serializable
{
    private static final long serialVersionUID = 1L;

    private final String _identifier;

    public MacaroonPrincipal(String id)
    {
        _identifier = requireNonNull(id);
    }

    @Override
    public String getName()
    {
        return _identifier;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + getName() + "]";
    }
}
