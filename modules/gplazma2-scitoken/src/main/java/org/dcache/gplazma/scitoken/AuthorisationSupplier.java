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
package org.dcache.gplazma.scitoken;

import java.util.Optional;

import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.MultiTargetedRestriction.Authorisation;

/**
 * An AuthorisationSupplier is something that may return a relocatable
 * Authorisation statement.
 */
@FunctionalInterface
public interface AuthorisationSupplier
{
    /**
     * Provides an authorisation statement.  There is a path argument to allow
     * the supplier to resolve any relative paths.
     * @param prefix The path to resolve relative paths.
     * @return Empty if no authorisation, or a non-null Authorisation statement.
     */
    Optional<Authorisation> authorisation(FsPath prefix);
}
