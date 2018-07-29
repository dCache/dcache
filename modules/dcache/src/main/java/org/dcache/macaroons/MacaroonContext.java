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
package org.dcache.macaroons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.LongStream;

import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.Activity;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.dcache.macaroons.InvalidCaveatException.checkCaveat;

/**
 * This class holds information that is (or is to be) encoded in a macaroon as
 * various caveats.
 */
public class MacaroonContext
{
    private static final Logger LOG = LoggerFactory.getLogger(MacaroonContext.class);

    private FsPath root = FsPath.ROOT;
    private FsPath home = FsPath.ROOT;
    private FsPath path = FsPath.ROOT;
    private String username;
    private OptionalLong maxUpload = OptionalLong.empty();
    private long uid = -1;
    private long[] gids;
    private EnumSet<Activity> activities = EnumSet.allOf(Activity.class);
    private String id;
    private Optional<Instant> expiry = Optional.empty();

    public void updateHome(String directory) throws InvalidCaveatException
    {
        LOG.debug("Updating home: {}", directory);
        home = root.resolve(directory);
    }

    public void setHome(FsPath path)
    {
        LOG.debug("Setting home to {}", path);
        home = path;
    }

    public Optional<FsPath> getHome()
    {
        return home == FsPath.ROOT ? Optional.empty() : Optional.of(home);
    }

    public void setRoot(FsPath newRoot)
    {
        LOG.debug("Setting root to {}", root);
        checkArgument(newRoot.hasPrefix(root), "Attempt to weaken root path");
        root = newRoot;
    }

    public void updateRoot(String directory) throws InvalidCaveatException
    {
        FsPath newRoot = root.chroot(directory);

        FsPath delta = FsPath.ROOT.resolve(directory);

        FsPath pathCompare = delta;
        if (path.length() < delta.length()) {
            pathCompare = delta.drop(delta.length() - path.length());
        }

        checkCaveat(path.hasPrefix(pathCompare), "root extends outside of path");
        path = FsPath.ROOT.resolve(path.stripPrefix(pathCompare));

        if (home.hasPrefix(delta)) {
            home = FsPath.ROOT.resolve(home.stripPrefix(delta));
        } else {
            home = FsPath.ROOT;
        }

        root = newRoot;
    }

    public Optional<FsPath> getRoot()
    {
        return root == FsPath.ROOT ? Optional.empty() : Optional.of(root);
    }

    public void setPath(FsPath desiredPath)
    {
        path = desiredPath;
        LOG.debug("Setting path to {}", path);
    }

    public void updatePath(String directory)
    {
        path = path.chroot(directory);
    }

    public Optional<FsPath> getPath()
    {
        return path == FsPath.ROOT ? Optional.empty() : Optional.of(path);
    }

    public void setUsername(String name)
    {
        checkArgument(!name.isEmpty(), "username is empty");
        checkArgument(username == null, "username is already set");
        username = name;
    }

    public String getUsername()
    {
        checkState(username != null, "username not specified");
        return username;
    }

    public void setUid(long id)
    {
        checkArgument(id >= 0, "invalid uid value");
        checkArgument(uid == -1, "uid is already set");
        uid = id;
    }

    public long getUid()
    {
        checkState(uid != -1, "uid not specified");
        return uid;
    }

    public void setGids(long[] id)
    {
        checkArgument(id.length > 0, "empty list of gids");
        checkArgument(gids == null, "gids are already set");
        gids = id;
    }

    public long[] getGids()
    {
        checkState(gids != null, "gid not specified");
        return gids;
    }

    public LongStream getGidStream()
    {
        return Arrays.stream(getGids());
    }

    public void updateAllowedActivities(EnumSet<Activity> newActivities) throws InvalidCaveatException
    {
        checkCaveat(activities.containsAll(newActivities), "attempt to enlarge activity set");
        activities = newActivities;
    }

    public void keepActivities(EnumSet<Activity> newActivities)
    {
        activities.retainAll(newActivities);
    }

    public void removeActivities(EnumSet<Activity> deniedActivities)
    {
        LOG.debug("Denying activities: {}", deniedActivities);
        activities.removeAll(deniedActivities);
    }

    public Optional<EnumSet<Activity>> getAllowedActivities()
    {
        /*  In general, any non-READ_METADATA activity only makes sense if
         *  the user is allowed to "enter a directory".  This requires the user
         *  isn't banned from the READ_METADATA activity, which would be the
         *  case if the "activity" caveat failed to include READ_METADATA
         *  activity.  Therefore, we treat the user's authorisation of any other
         *  activity as an implicit authorisation of the READ_METADATA activity.
         *
         *  Note that other caveats can impose additional Restrictions that
         *  limit READ_METADATA; e.g., the path caveat.  Implicitly authorising
         *  READ_METADATA in allowed activities does not stop those other
         *  Restrictions from being effective.
         */

        if (activities.size() == Activity.values().length ||
                activities.size() == Activity.values().length-1 && !activities.contains(Activity.READ_METADATA)) {
            // As an optimisation, treat an authorisation of all activities as
            // if the macaroon has no activity caveat.
            return Optional.empty();
        } else if (activities.isEmpty()) {
            return Optional.of(EnumSet.noneOf(Activity.class));
        } else {
            EnumSet<Activity> a = EnumSet.copyOf(activities);
            a.add(Activity.READ_METADATA);
            return Optional.of(a);
        }
    }

    public void setId(String id)
    {
        this.id = requireNonNull(id);
    }

    public String getId()
    {
        checkState(id != null, "Missing identifier");
        return id;
    }

    public void updateExpiry(Instant newExpiry)
    {
        requireNonNull(newExpiry);

        if (!expiry.isPresent() || newExpiry.isBefore(expiry.get())) {
            expiry = Optional.of(newExpiry);
            LOG.debug("Updating expiry to {}", newExpiry);
        }
    }

    public Optional<Instant> getExpiry()
    {
        return expiry;
    }

    public void updateMaxUpload(long value) throws InvalidCaveatException
    {
        checkCaveat(value > 0, CaveatType.MAX_UPLOAD.getLabel() + " must be a positive value");
        long updatedValue = maxUpload.isPresent() ? Math.min(maxUpload.getAsLong(), value) : value;
        maxUpload = OptionalLong.of(updatedValue);
    }

    public OptionalLong getMaxUpload()
    {
        return maxUpload;
    }
}
