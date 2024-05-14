/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020-2023 Deutsches Elektronen-Synchrotron
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

import static java.util.Arrays.asList;
import static org.dcache.auth.attributes.Activity.*;
import static org.dcache.gplazma.oidc.profiles.InvalidScopeException.checkScopeValid;

import com.google.common.collect.ImmutableMap;
import diskCacheV111.util.FsPath;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.MultiTargetedRestriction;
import org.dcache.auth.attributes.MultiTargetedRestriction.Authorisation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Scope represents one of the space-separated list of allowed operations contained within a WLCG
 * Profile "scope" claim.
 * <p/>
 * For details, see https://zenodo.org/record/3460258#.XefiHKbTVhF
 */
public class WlcgProfileScope implements AuthorisationSupplier {

    /**
     * Which kind of request the scope authorises.
     */
    public enum Operation {
        /**
         * Read data. Only applies to “online” resources such as disk (as opposed to “nearline” such
         * as tape where the {@literal stage} authorization should be used in addition).
         */
        READ("storage.read", true, LIST, READ_METADATA, DOWNLOAD),

        /**
         * Upload data. This includes renaming files if the destination file does not already exist.
         * This capability includes the creation of directories and subdirectories at the specified
         * path, and the creation of any non-existent directories required to create the path itself
         * (note the server implementation MUST NOT automatically create directories for a client).
         * This authorization DOES NOT permit overwriting or deletion of stored data. The driving
         * use case for a separate {@literal storage.create} scope is to enable stage-out of data
         * from jobs on a worker node.
         */
        CREATE("storage.create", true, READ_METADATA, UPLOAD, MANAGE, UPDATE_METADATA),

        /**
         * Change data. This includes renaming files, creating new files, and writing data. This
         * permission includes overwriting or replacing stored data in addition to deleting or
         * truncating data. This is a strict superset of {@literal storage.create}.
         */
        MODIFY("storage.modify", true, LIST, READ_METADATA, UPLOAD, MANAGE, DELETE, UPDATE_METADATA),

        /**
         * Read the data, potentially causing data to be staged from a nearline resource to an
         * online resource. This is a superset of {@literal storage.read}.
         */
        STAGE("storage.stage", true, LIST, READ_METADATA, DOWNLOAD, Activity.STAGE),

        /**
         * "Read" or query information about job status and attributes.
         */
        COMPUTE_READ("compute.read", false),

        /**
         * Modify or change the attributes of an existing job.
         */
        COMPUTE_MODIFY("compute.modify", false),

        /**
         * Create or submit a new job at the computing resource.
         */
        COMPUTE_CREATE("compute.create", false),

        /**
         * Delete a job from the computing resource, potentially terminating
         * a running job.
         */
        COMPUTE_CANCEL("compute.cancel", false);

        private final String label;
        private final EnumSet<Activity> allowedActivities;

        private final boolean requirePath;

        private Operation(String label, boolean requirePath, Activity... allowedActivities) {
            this.label = label;
            this.requirePath = requirePath;
            this.allowedActivities = allowedActivities.length == 0
                    ? EnumSet.noneOf(Activity.class)
                    : EnumSet.copyOf(asList(allowedActivities));
        }

        public String getLabel() {
            return label;
        }

        public EnumSet<Activity> allowedActivities() {
            return allowedActivities;
        }

        public boolean isPathRequired() {
            return requirePath;
        }
    }

    private static final Map<String, Operation> OPERATIONS_BY_LABEL;
    private static final int MINIMUM_LABEL_SIZE;
    private static final Logger LOGGER = LoggerFactory.getLogger(WlcgProfileScope.class);

    static {
        ImmutableMap.Builder<String, Operation> builder = ImmutableMap.builder();
        Arrays.stream(Operation.values()).forEach(o -> builder.put(o.getLabel(), o));
        OPERATIONS_BY_LABEL = builder.build();
        MINIMUM_LABEL_SIZE = OPERATIONS_BY_LABEL.keySet().stream().mapToInt(String::length).min()
              .orElse(0);
    }

    private final Operation operation;
    private final String path;

    public WlcgProfileScope(String scope) {
        int colon = scope.indexOf(':');

        String operationLabel = colon == -1 ? scope : scope.substring(0, colon);

        operation = OPERATIONS_BY_LABEL.get(operationLabel);
        checkScopeValid(operation != null, "Unknown operation %s", operationLabel);

        if (colon == -1) {
            checkScopeValid(!operation.isPathRequired(), "Path must be specified");
            path = "/";
        } else {
            String scopePath = scope.substring(colon + 1);
            checkScopeValid(scopePath.startsWith("/"), "Path does not start with /");

            path = URI.create(scopePath).getPath();
        }

        LOGGER.debug("WlcgProfileScope created from scope \"{}\": op={} path={}",
              scope, operation, path);
    }

    public static boolean isWlcgProfileScope(String scope) {
        int colon = scope.indexOf(':');
        String authz = colon == -1 ? scope : scope.substring(0, colon);
        return authz.length() >= MINIMUM_LABEL_SIZE
              && OPERATIONS_BY_LABEL.keySet().contains(authz);
    }

    @Override
    public Optional<MultiTargetedRestriction.Authorisation> authorisation(FsPath prefix) {
        if (operation.allowedActivities.isEmpty()) {
            return Optional.empty();
        }

        FsPath absPath = prefix.resolve(path.substring(1));
        LOGGER.debug("WlcgProfileScope authorising {} with prefix \"{}\" to path {}",
              prefix, operation.allowedActivities, absPath);
        return Optional.of(new Authorisation(operation.allowedActivities, absPath));
    }
}
