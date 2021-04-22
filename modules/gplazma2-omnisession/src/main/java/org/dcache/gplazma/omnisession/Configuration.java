/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.omnisession;

import java.security.Principal;
import java.util.List;
import java.util.Set;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.gplazma.AuthenticationException;

/**
 * The configuration for the omnisession plugin.
 */
public interface Configuration
{
    /**
     * Provide a list of LoginAttributes for the user identified by the
     * supplied set of principals.
     * @param principals The identity of the user
     * @return the LoginAttributes for this user
     * @throws AuthenticationException if there is a problem processing this request.
     */
    List<LoginAttribute> attributesFor(Set<Principal> principals)
            throws AuthenticationException;
}
