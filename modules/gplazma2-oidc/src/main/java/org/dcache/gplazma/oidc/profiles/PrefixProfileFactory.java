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
package org.dcache.gplazma.oidc.profiles;

import diskCacheV111.util.FsPath;
import java.util.Map;
import org.dcache.gplazma.oidc.Profile;
import org.dcache.gplazma.oidc.ProfileFactory;

/**
 * Abstract ProfileFactory for supporting ProfileFactory classes that create profiles that
 * require a prefix argument.
 */
abstract class PrefixProfileFactory<T extends Profile> implements ProfileFactory {

    @Override
    public T create(Map<String, String> arguments) {

        String prefixConfig = arguments.get("prefix");

        if (prefixConfig == null) {
            throw new IllegalArgumentException("Missing 'prefix' argument.");
        }

        if (!prefixConfig.startsWith("/")) {
            throw new IllegalArgumentException("Path in 'prefix' is not absolute: must start with '/'");
        }

        FsPath prefix = FsPath.create(prefixConfig);
        return createProfile(prefix, arguments);
    }

    abstract protected T createProfile(FsPath prefix, Map<String,String> arguments);
}
