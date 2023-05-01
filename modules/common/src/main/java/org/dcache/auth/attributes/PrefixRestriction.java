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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import diskCacheV111.util.FsPath;
import java.io.IOException;
import java.util.function.Function;

/**
 * A Restriction that allows a user to perform only activity on paths with a particular prefix.
 */
public class PrefixRestriction implements Restriction {

    private static final long serialVersionUID = 7073397935939729478L;

    private ImmutableSet<FsPath> prefixes;

    private transient Function<FsPath, FsPath> resolver;

    public PrefixRestriction(FsPath... prefixes) {
        this.prefixes = ImmutableSet.copyOf(prefixes);
    }

    public ImmutableSet<FsPath> getPrefixes() {
        return prefixes;
    }

    @Override
    public boolean isRestricted(Activity activity, FsPath path) {
        for (FsPath prefix : prefixes) {
            Function<FsPath, FsPath> resolver = getPathResolver();
            prefix = resolver.apply(prefix);
            path = resolver.apply(path);
            if (path.hasPrefix(prefix)) {
                return false;
            }
            if (prefix.hasPrefix(path) && (activity == Activity.READ_METADATA
                  || activity == Activity.LIST)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isRestricted(Activity activity, FsPath directory, String child) {
        return isRestricted(activity, directory.child(child));
    }

    @Override
    public boolean hasUnrestrictedChild(Activity activity, FsPath parent) {
        Function<FsPath, FsPath> resolver = getPathResolver();
        FsPath resolvedParent = resolver.apply(parent);
        return prefixes.stream().map(resolver::apply)
              .anyMatch(p -> p.hasPrefix(resolvedParent) && !p.equals(resolvedParent));
    }

    @Override
    public int hashCode() {
        return PrefixRestriction.class.hashCode() ^ prefixes.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof PrefixRestriction && ((PrefixRestriction) other).prefixes.equals(
              prefixes);
    }

    @Override
    public boolean isSubsumedBy(Restriction other) {
        return other instanceof PrefixRestriction && ((PrefixRestriction) other).subsumes(this);
    }

    @Override
    public void setPathResolver(Function<FsPath, FsPath> resolver) {
        this.resolver = resolver;
    }

    @Override
    public Function<FsPath, FsPath> getPathResolver() {
        return resolver != null ? resolver : Restriction.super.getPathResolver();
    }


    private boolean subsumes(PrefixRestriction restriction) {
        for (FsPath prefix : prefixes) {
            if (restriction.isRestricted(prefix)) {
                return false;
            }
        }
        return true;
    }

    private boolean isRestricted(FsPath path) {
        for (FsPath prefix : prefixes) {
            Function<FsPath, FsPath> resolver = getPathResolver();
            prefix = resolver.apply(prefix);
            path = resolver.apply(path);
            if (path.hasPrefix(prefix)) {
                return false;
            }
        }
        return true;
    }

    private void readObject(java.io.ObjectInputStream stream)
          throws IOException, ClassNotFoundException {
        int countPrefixes = stream.readInt();
        ImmutableSet.Builder<FsPath> builder = ImmutableSet.builder();
        for (int i = 0; i < countPrefixes; i++) {
            builder.add(FsPath.create(stream.readObject().toString()));
        }
        prefixes = builder.build();
    }

    private void writeObject(java.io.ObjectOutputStream stream)
          throws IOException {
        stream.writeInt(prefixes.size());
        for (FsPath prefix : prefixes) {
            stream.writeObject(prefix.toString());
        }
    }

    /**
     * @return string representation of prefixes before resolution
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PrefixRestrict[");

        if (prefixes.size() == 1) {
            sb.append("prefix=").append(prefixes.iterator().next().toString());
        } else {
            sb.append("prefixes={").append(Joiner.on(',').join(prefixes)).append('}');
        }
        return sb.append(']').toString();
    }
}
