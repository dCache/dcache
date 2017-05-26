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

import java.util.Collection;

import diskCacheV111.util.FsPath;

/**
 *  Various utility classes to handle LoginAttributes
 */
public class LoginAttributes
{

    private LoginAttributes()
    {
        // prevent instantiation
    }

    public static FsPath getUserRoot(Collection<LoginAttribute> attributes)
    {
        return attributes.stream()
                .filter(RootDirectory.class::isInstance)
                .map(RootDirectory.class::cast)
                .map(RootDirectory::getRoot)
                .map(FsPath::create)
                .findFirst()
                .orElse(FsPath.ROOT);
    }
}
