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
import java.util.OptionalLong;
import java.util.stream.Stream;

import diskCacheV111.util.FsPath;

/**
 *  Various utility classes to handle LoginAttributes
 */
public class LoginAttributes
{
    public static final String ADMIN_ROLE_NAME = "admin";
    public static final String OBSERVER_ROLE_NAME = "observer";
    private static final Role ADMIN_ROLE = new Role(ADMIN_ROLE_NAME);
    private static final Role OBSERVER_ROLE = new Role(OBSERVER_ROLE_NAME);

    private LoginAttributes()
    {
        // prevent instantiation
    }

    /**
     * Provide the root directory for this user.  The value takes into account
     * any Role attributes that may adjust the user's root directory.
     */
    public static FsPath getUserRoot(Collection<LoginAttribute> attributes)
    {
        FsPath root = FsPath.ROOT;
        for (LoginAttribute attribute : attributes) {
            if (attribute.equals(ADMIN_ROLE) || attribute.equals(OBSERVER_ROLE)) {
                return FsPath.ROOT;
            }
            if (attribute instanceof RootDirectory) {
                root = FsPath.create(((RootDirectory)attribute).getRoot());
            }
        }
        return root;
    }

    public static Role adminRole()
    {
        return ADMIN_ROLE;
    }

    public static Role observerRole()
    {
        return OBSERVER_ROLE;
    }

    public static boolean hasAdminRole(Collection<LoginAttribute> attributes)
    {
        return attributes.stream().anyMatch(ADMIN_ROLE::equals);
    }

    public static boolean hasObserverRole(Collection<LoginAttribute> attributes)
    {
        return attributes.stream().anyMatch(OBSERVER_ROLE::equals);
    }

    public static Stream<String> assertedRoles(Collection<LoginAttribute> attributes)
    {
        return attributes.stream()
                .filter(Role.class::isInstance)
                .map(Role.class::cast)
                .map(Role::getRole);
    }

    public static Stream<String> unassertedRoles(Collection<LoginAttribute> attributes)
    {
        return attributes.stream()
                .filter(UnassertedRole.class::isInstance)
                .map(UnassertedRole.class::cast)
                .map(UnassertedRole::getRole);
    }

    public static OptionalLong maximumUploadSize(Collection<LoginAttribute> attributes)
    {
        return attributes.stream()
                .filter(MaxUploadSize.class::isInstance)
                .map(MaxUploadSize.class::cast)
                .mapToLong(a -> a.getMaximumSize())
                .min();
    }
}
