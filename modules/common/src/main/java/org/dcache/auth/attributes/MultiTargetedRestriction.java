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
package org.dcache.auth.attributes;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import diskCacheV111.util.FsPath;
import java.io.Serializable;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class represents a restriction that only allows certain activities with specific paths.
 */
public class MultiTargetedRestriction implements Restriction {

    private static final long serialVersionUID = -4604505680192926544L;

    private static final EnumSet<Activity> ALLOWED_PARENT_ACTIVITIES
          = EnumSet.of(Activity.LIST, Activity.READ_METADATA);

    /**
     * An Authorisation is a set of activities that are allowed for some path and its children.
     */
    public static class Authorisation implements Serializable, Comparable<Authorisation> {

        private static final long serialVersionUID = 1L;

        private final EnumSet<Activity> activities;
        private final FsPath path;

        public Authorisation(Collection<Activity> activities, FsPath path) {
            this.activities = EnumSet.copyOf(activities);
            this.path = path;
        }

        public EnumSet<Activity> getActivity() {
            return activities;
        }

        public FsPath getPath() {
            return path;
        }

        @Override
        public int hashCode() {
            return activities.hashCode() ^ path.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }

            if (!(other instanceof Authorisation)) {
                return false;
            }

            Authorisation otherAuthorisation = (Authorisation) other;
            return otherAuthorisation.activities.equals(activities)
                  && otherAuthorisation.path.equals(path);
        }

        @Override
        public int compareTo(Authorisation other) {
            return ComparisonChain.start()
                  .compare(this.path, other.path, Ordering.usingToString())
                  .compare(this.activities, other.activities, Ordering.natural().lexicographical())
                  .result();
        }

        @Override
        public String toString() {
            return "Authorisation{allowing " + activities + " on " + path + "}";
        }
    }

    private final Collection<Authorisation> authorisations;

    private transient Function<FsPath, FsPath> resolver;

    /**
     * Create a Restriction based on the supplied collection of Authorisations. An Authorisation
     * with the path FsPath.ROOT implies its activities are unrestricted.  If no authorisation has a
     * particular activity then that activity is restricted for all paths.
     */
    public MultiTargetedRestriction(Collection<Authorisation> authorisations) {
        // Sort authorisations to form a canonical ordering.  This simplifies
        // MultiTargetedRestriction#hashCode and #equals methods.
        this.authorisations = authorisations.stream().sorted().collect(toImmutableList());
    }

    public MultiTargetedRestriction alsoAuthorising(Collection<Authorisation> authz) {
        Set<Authorisation> combined = new HashSet(authorisations);
        combined.addAll(authz);
        return new MultiTargetedRestriction(combined);
    }

    @Override
    public boolean hasUnrestrictedChild(Activity activity, FsPath parent) {
        for (Authorisation authorisation : authorisations) {
            Function<FsPath, FsPath> resolver = getPathResolver();
            FsPath allowedPath = resolver.apply(authorisation.getPath());
            EnumSet<Activity> allowedActivity = authorisation.getActivity();
            parent = resolver.apply(parent);

            /*  As an example, if allowedPath is /path/to/dir then we return
             *  true if parent is /path or if parent is /path/to/dir/my-data,
             *  but return false if parent is /path/to/other/dir.
             */
            if (allowedActivity.contains(activity) &&
                  (allowedPath.hasPrefix(parent) || parent.hasPrefix(allowedPath))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isRestricted(Activity activity, FsPath path) {
        return isRestricted(activity, path, false);
    }

    @Override
    public boolean isRestricted(Activity activity, FsPath directory, String child,
          boolean skipPrefixCheck) {
        return isRestricted(activity, directory.child(child), skipPrefixCheck);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof MultiTargetedRestriction)) {
            return false;
        }

        return ((MultiTargetedRestriction) other).authorisations.equals(authorisations);
    }

    @Override
    public int hashCode() {
        return authorisations.hashCode();
    }

    /**
     * Check whether this restriction subsumes the other restriction.  Return true if this
     * restriction is as restrictive or more restrictive than other.
     */
    private boolean subsumes(MultiTargetedRestriction other) {
        return authorisations.stream()
              .allMatch(ap -> other.hasAuthorisationSubsumedBy(ap));
    }

    /**
     * Return true iff this restriction has an Authorisation that is subsumed by other.
     */
    private boolean hasAuthorisationSubsumedBy(Authorisation other) {
        EnumSet<Activity> disallowedOtherActivities = EnumSet.complementOf(other.activities);
        Function<FsPath, FsPath> resolver = getPathResolver();
        return authorisations.stream()
              .anyMatch(
                    ap -> disallowedOtherActivities.containsAll(EnumSet.complementOf(ap.activities))
                          && resolver.apply(other.getPath()).hasPrefix(resolver.apply(ap.getPath())));
    }

    @Override
    public boolean isSubsumedBy(Restriction other) {
        if (other instanceof MultiTargetedRestriction) {
            return ((MultiTargetedRestriction) other).subsumes(this);
        }

        return false;
    }

    @Override
    public void setPathResolver(Function<FsPath, FsPath> resolver) {
        this.resolver = resolver;
    }

    @Override
    public Function<FsPath, FsPath> getPathResolver() {
        return resolver != null ? resolver : Restriction.super.getPathResolver();
    }


    @Override
    public String toString() {
        return authorisations.stream()
              .map(Object::toString)
              .collect(Collectors.joining(", ", "MultiTargetedRestriction[", "]"));
    }

    private boolean isRestricted(Activity activity, FsPath path, boolean skipPrefixCheck) {
        for (Authorisation authorisation : authorisations) {
            EnumSet<Activity> allowedActivity = authorisation.getActivity();
            if (skipPrefixCheck) {
                if (allowedActivity.contains(activity)) {
                    return false;
                }

                if (ALLOWED_PARENT_ACTIVITIES.contains(activity)) {
                    return false;
                }
            } else {
                FsPath allowedPath = getPathResolver().apply(authorisation.getPath());
                path = getPathResolver().apply(path);
                if (allowedActivity.contains(activity) && path.hasPrefix(allowedPath)) {
                    return false;
                }

                // As a special case, certain activities are always allowed for
                // parents of an AllowedPath.
                if (ALLOWED_PARENT_ACTIVITIES.contains(activity) && allowedPath.hasPrefix(path)) {
                    return false;
                }
            }
        }

        return true;
    }
}
