/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.oidc;

import java.util.Map;

/**
 * A class that implements ProfileFactory describes how to create a Profile.
 * @see Profile
 */
public interface ProfileFactory {

    /**
     * Provide a new Profile based on the supplied arguments.  A profile factory may tailor the
     * behaviour of the profile based on admin-supplied arguments.  As the caller does not know
     * which arguments are relevant to this profile, all arguments are provided.
     * @param arguments  Admin-provided arguments that may be significant for this profile.
     * @return A Profile that will process OIDC claims.
     */
    Profile create(Map<String,String> arguments);
}
