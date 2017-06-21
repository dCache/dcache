/*
 * dCache - http://www.dcache.org/
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
package org.dcache.gplazma.roles;

import com.google.common.annotations.VisibleForTesting;

import java.security.Principal;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.dcache.auth.DesiredRole;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.attributes.Role;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A plugin for processing a user's DesiredRole principals and, if authorised,
 * adding the corresponding Role.
 */
public class RolesPlugin implements GPlazmaSessionPlugin
{
    @VisibleForTesting
    static final String ADMIN_GID_PROPERTY_NAME = "gplazma.roles.admin-gid";

    private final long adminGid;

    public RolesPlugin(Properties properties)
    {
        String adminGidProperty = properties.getProperty(ADMIN_GID_PROPERTY_NAME);
        checkArgument(adminGidProperty != null, "Undefined property: " + ADMIN_GID_PROPERTY_NAME);
        try {
            this.adminGid = Long.parseLong(adminGidProperty, 10);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Badly formatted " +
                    ADMIN_GID_PROPERTY_NAME + " value: " + e);
        }
    }

    @Override
    public void session(Set<Principal> principals, Set<Object> attributes)
            throws AuthenticationException
    {
        List<String> desiredRoles = principals.stream()
                .filter(DesiredRole.class::isInstance)
                .map(DesiredRole.class::cast)
                .map(DesiredRole::getName)
                .collect(Collectors.toList());

        List<String> unauthorizedRoles = desiredRoles.stream()
                .filter(r -> !isAuthorized(r, principals))
                .collect(Collectors.toList());

        if (!unauthorizedRoles.isEmpty()) {
            String description = unauthorizedRoles.size() == 1
                    ? unauthorizedRoles.get(0)
                    : unauthorizedRoles.stream().collect(Collectors.joining(",", "[", "]"));
            throw new AuthenticationException("unauthorized for " + description);
        }

        desiredRoles.stream().map(Role::new).forEach(attributes::add);
    }

    private boolean isAuthorized(String role, Set<Principal> principals)
    {
        switch (role) {
        case "admin":
            return principals.stream()
                    .filter(GidPrincipal.class::isInstance)
                    .map(GidPrincipal.class::cast)
                    .mapToLong(GidPrincipal::getGid)
                    .anyMatch(gid -> gid == adminGid);
        }
        return false;
    }
}
