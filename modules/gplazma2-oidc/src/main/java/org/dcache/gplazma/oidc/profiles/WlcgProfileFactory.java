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

import com.google.common.base.Splitter;
import diskCacheV111.util.FsPath;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dcache.auth.Subjects;

/**
 * ProfileFactory for creating a Profile that supports the AuthZ-WG profile.
 */
public class WlcgProfileFactory extends PrefixProfileFactory<WlcgProfile> {

    @Override
    protected WlcgProfile createProfile(FsPath prefix, Map<String, String> arguments) {
        Set<Principal> authzIdentity = parsePrincipals(arguments, "authz-id");
        Set<Principal> nonAuthzIdentity = parsePrincipals(arguments, "non-authz-id");
        return new WlcgProfile(prefix, authzIdentity, nonAuthzIdentity);
    }

    private Set<Principal> parsePrincipals(Map<String, String> arguments, String key) {
        String value = arguments.get(key);
        if (value == null) {
            return Collections.emptySet();
        }

        List<String> items = Splitter.on(' ').omitEmptyStrings().trimResults().splitToList(value);
        if (items.isEmpty()) {
            return Collections.emptySet();
        }

        try {
            return Subjects.principalsFromArgs(items);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Bad " + key + " value: " + e.getMessage());
        }
    }
}
