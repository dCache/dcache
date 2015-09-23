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

import com.google.common.base.Splitter;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import org.dcache.auth.attributes.Activity;

import static org.dcache.macaroons.InvalidCaveatException.checkCaveat;

/**
 * Utility class for handling caveat values.
 */
public class CaveatValues
{
    private CaveatValues()
    {
        // prevent instantiation.
    }

    public static String asIdentityCaveatValue(MacaroonContext context)
    {
        String gidList = Arrays.stream(context.getGids()).mapToObj(Long::toString).collect(Collectors.joining(","));
        return context.getUid() + ";" + gidList + ";" + context.getUsername();
    }

    public static void parseIdentityCaveatValue(MacaroonContext context, String value) throws InvalidCaveatException
    {
        List<String> idElements = Splitter.on(';').limit(3).splitToList(value);
        checkCaveat(idElements.size() == 3, "Missing elements");

        try {
            context.setUid(Long.valueOf(idElements.get(0)));
        } catch (NumberFormatException e) {
            throw InvalidCaveatException.wrap("Badly formatted uid", e);
        }

        List<String> gids = Splitter.on(',').splitToList(idElements.get(1));
        try {
            context.setGids(gids.stream().mapToLong(Long::valueOf).toArray());
        } catch (NumberFormatException e) {
            throw InvalidCaveatException.wrap("Badly formatted gid", e);
        }

        String username = idElements.get(2).trim();
        checkCaveat(!username.isEmpty(), "empty username not allowed");

        context.setUsername(username);
    }

    public static String asActivityCaveatValue(EnumSet<Activity> allowedActivities)
    {
        return allowedActivities.stream().map(Activity::name).collect(Collectors.joining(","));
    }

    public static EnumSet<Activity> parseActivityCaveatValue(String value) throws InvalidCaveatException
    {
        EnumSet<Activity> activities = EnumSet.noneOf(Activity.class);
        for (String activity : Splitter.on(',').trimResults().split(value)) {
            try {
                activities.add(Activity.valueOf(activity));
            } catch (IllegalArgumentException e) {
                throw InvalidCaveatException.wrap("Bad activity value", e);
            }
        }
        return activities;
    }
}
