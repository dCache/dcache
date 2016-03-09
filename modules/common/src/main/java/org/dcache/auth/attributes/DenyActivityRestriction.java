/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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


import com.google.common.base.Joiner;

import java.util.EnumSet;

import diskCacheV111.util.FsPath;

/**
 * A Restriction that allows a user to perform only activity from the
 * supplied set of activities.
 */
public class DenyActivityRestriction implements Restriction
{
    private static final long serialVersionUID = 1L;

    private final EnumSet<Activity> denied;

    public static DenyActivityRestriction restrictAllActivity()
    {
        return new DenyActivityRestriction(EnumSet.allOf(Activity.class));
    }

    public static DenyActivityRestriction restrictNoActivity()
    {
        return new DenyActivityRestriction(EnumSet.noneOf(Activity.class));
    }

    public DenyActivityRestriction(Activity... denied)
    {
        this.denied = (denied.length > 0)
                      ? EnumSet.of(denied[0], denied)
                      : EnumSet.noneOf(Activity.class);
    }

    private DenyActivityRestriction(EnumSet<Activity> denied)
    {
        this.denied = denied;
    }

    @Override
    public boolean isRestricted(Activity activity, FsPath path)
    {
        return denied.contains(activity);
    }

    @Override
    public boolean isRestricted(Activity activity, FsPath directory, String name)
    {
        return denied.contains(activity);
    }

    @Override
    public int hashCode()
    {
        return denied.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof DenyActivityRestriction && denied.equals(((DenyActivityRestriction) other).denied);
    }

    @Override
    public boolean isSubsumedBy(Restriction other)
    {
        if (!(other instanceof DenyActivityRestriction)) {
            return false;
        }

        EnumSet<Activity> otherDenied = ((DenyActivityRestriction) other).denied;

        return otherDenied.containsAll(denied);
    }

    @Override
    public String toString()
    {
        if (denied.isEmpty()) {
            return "Unrestricted";
        }
        if (denied.size() == Activity.values().length) {
            return "FullyRestricted";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Restrict[");
        Joiner.on(',').appendTo(sb, denied);
        sb.append(']');
        return sb.toString();
    }
}
