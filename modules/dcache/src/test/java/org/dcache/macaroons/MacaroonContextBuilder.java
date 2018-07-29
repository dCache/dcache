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

import java.time.Instant;
import java.util.EnumSet;

import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.Activity;

import static java.util.Arrays.asList;

/**
 * Fluent class to support writing shorter and easier-to-read unit-test.
 */
public class MacaroonContextBuilder
{
    private final MacaroonContext context = new MacaroonContext();

    public static MacaroonContextBuilder macaroonContext()
    {
        return new MacaroonContextBuilder();
    }

    public MacaroonContext build()
    {
        return context;
    }

    public MacaroonContextBuilder withUid(long id)
    {
        context.setUid(id);
        return this;
    }

    public MacaroonContextBuilder withGid(long... ids)
    {
        context.setGids(ids);
        return this;
    }

    public MacaroonContextBuilder withUsername(String name)
    {
        context.setUsername(name);
        return this;
    }

    public MacaroonContextBuilder withRoot(String path)
    {
        context.setRoot(FsPath.create(path));
        return this;
    }

    public MacaroonContextBuilder withPath(String path)
    {
        context.setPath(FsPath.create(path));
        return this;
    }

    public MacaroonContextBuilder withHome(String path)
    {
        context.setHome(FsPath.create(path));
        return this;
    }

    public MacaroonContextBuilder withAllowedActivities(Activity... activities) throws InvalidCaveatException
    {
        context.updateAllowedActivities(EnumSet.copyOf(asList(activities)));
        return this;
    }

    public MacaroonContextBuilder withExpiry(Instant expiry)
    {
        context.updateExpiry(expiry);
        return this;
    }

    public MacaroonContextBuilder withMaxUpload(long value) throws InvalidCaveatException
    {
        context.updateMaxUpload(value);
        return this;
    }
}
