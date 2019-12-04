/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019-2020 Deutsches Elektronen-Synchrotron
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

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.MultiTargetedRestriction.Authorisation;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.dcache.auth.attributes.Activity.*;
import static org.dcache.gplazma.scitoken.InvalidScopeException.checkScopeValid;

/**
 * A Scope represents one of the space-separated list of allowed operations
 * contained within the SciToken "scope" claim.
 */
public class SciTokenScope implements AuthorisationSupplier
{
    public enum Operation
    {
        READ(LIST, READ_METADATA, DOWNLOAD),
        WRITE(LIST, READ_METADATA, UPLOAD, MANAGE, DELETE, UPDATE_METADATA),
        QUEUE,
        EXECUTE;

        private final EnumSet<Activity> allowedActivities;

        private Operation(Activity... allowedActivities)
        {
            this.allowedActivities = allowedActivities.length == 0
                    ? EnumSet.noneOf(Activity.class)
                    : EnumSet.copyOf(asList(allowedActivities));
        }
    }

    private static final String SCITOKEN_SCOPE_PREFIX = "https://scitokens.org/v1/authz/";
    private static final Map<String,Operation> OPERATIONS_BY_LABEL;
    private static final Logger LOGGER = LoggerFactory.getLogger(SciTokenScope.class);

    static {
        ImmutableMap.Builder<String,Operation> builder = ImmutableMap.builder();
        Arrays.stream(Operation.values()).forEach(o -> builder.put(o.toString().toLowerCase(), o));
        OPERATIONS_BY_LABEL = builder.build();
    }

    private final Operation operation;
    private final String path;

    SciTokenScope(Operation operation, String path)
    {
        this.operation = requireNonNull(operation);
        this.path = requireNonNull(path);
        LOGGER.debug("SciTokenScope created: op={} path={}", operation, path);
    }

    public SciTokenScope(String scope) throws InvalidScopeException
    {
        String withoutPrefix = withoutPrefix(scope);

        int colon = withoutPrefix.indexOf(':');

        String operationLabel = colon == -1 ? withoutPrefix : withoutPrefix.substring(0, colon);
        operation = OPERATIONS_BY_LABEL.get(operationLabel);
        checkScopeValid(operation != null, "Unknown operation %s", operationLabel);

        if (colon == -1) {
            path = "/";
        } else {
            path = withoutPrefix.substring(colon+1);
            checkScopeValid(path.startsWith("/"), "Path does not start with /");
        }

        LOGGER.debug("SciTokenScope created from scope \"{}\": op={} path={}",
                scope, operation, path);
    }

    public static boolean isSciTokenScope(String scope)
    {
        String withoutPrefix = withoutPrefix(scope);
        int colon = withoutPrefix.indexOf(':');
        String operation = colon == -1 ? withoutPrefix : withoutPrefix.substring(0, colon);

        return (colon == -1 || withoutPrefix.substring(colon+1).startsWith("/"))
                && OPERATIONS_BY_LABEL.keySet().contains(operation);
    }

    private static String withoutPrefix(String scope)
    {
        return scope.startsWith(SCITOKEN_SCOPE_PREFIX)
                ? scope.substring(SCITOKEN_SCOPE_PREFIX.length()) : scope;
    }

    @Override
    public Optional<Authorisation> authorisation(FsPath prefix)
    {
        FsPath absPath = prefix.resolve(path.substring(1));
        LOGGER.debug("SciTokenScope authorising {} with prefix \"{}\" to path {}",
                operation.allowedActivities, prefix, absPath);
        return operation.allowedActivities.isEmpty()
                ? Optional.empty()
                : Optional.of(new Authorisation(operation.allowedActivities, absPath));
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof SciTokenScope)) {
            return false;
        }

        SciTokenScope otherScope = (SciTokenScope) other;
        return otherScope.operation == operation && otherScope.path.equals(path);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + Objects.hashCode(operation);
        hash = 97 * hash + Objects.hashCode(path);
        return hash;
    }
}
