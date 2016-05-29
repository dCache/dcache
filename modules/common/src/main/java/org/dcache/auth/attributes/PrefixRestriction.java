/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

import com.google.common.collect.ImmutableSet;

import java.io.IOException;

import diskCacheV111.util.FsPath;

/**
 * A Restriction that allows a user to perform only activity on paths with a
 * particular prefix.
 */
public class PrefixRestriction implements Restriction
{
    private static final long serialVersionUID = 7073397935939729478L;

    private ImmutableSet<FsPath> prefixes;

    public PrefixRestriction(FsPath... prefixes)
    {
        this.prefixes = ImmutableSet.copyOf(prefixes);
    }

    @Override
    public boolean isRestricted(Activity activity, FsPath path)
    {
        for (FsPath prefix : prefixes) {
            if (path.hasPrefix(prefix) || activity == Activity.READ_METADATA && prefix.hasPrefix(path)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isRestricted(Activity activity, FsPath directory, String child)
    {
        return isRestricted(activity, directory.child(child));
    }

    @Override
    public int hashCode()
    {
        return PrefixRestriction.class.hashCode() ^ prefixes.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof PrefixRestriction && ((PrefixRestriction) other).prefixes.equals(prefixes);
    }

    @Override
    public boolean isSubsumedBy(Restriction other)
    {
        return other instanceof PrefixRestriction && ((PrefixRestriction) other).subsumes(this);
    }

    private boolean subsumes(PrefixRestriction restriction)
    {
        for (FsPath prefix : prefixes) {
            if (restriction.isRestricted(prefix)) {
                return false;
            }
        }
        return true;
    }

    private boolean isRestricted(FsPath path)
    {
        for (FsPath prefix : prefixes) {
            if (path.hasPrefix(prefix)) {
                return false;
            }
        }
        return true;
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        int count = stream.readInt();
        ImmutableSet.Builder<FsPath> builder = ImmutableSet.builder();
        for (int i = 0; i < count; i++) {
            builder.add(FsPath.create(stream.readObject().toString()));
        }
        prefixes = builder.build();
    }

    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException
    {
        stream.writeInt(prefixes.size());
        for (FsPath prefix : prefixes) {
            stream.writeObject(prefix.toString());
        }
    }
}
