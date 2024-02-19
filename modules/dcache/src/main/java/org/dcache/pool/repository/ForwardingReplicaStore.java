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
package org.dcache.pool.repository;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.nio.file.OpenOption;
import java.util.Set;
import org.dcache.util.Strings;

/**
 * A ReplicaStore that delegates all operations to some delegate.
 */
public abstract class ForwardingReplicaStore implements ReplicaStore {

    private static final String NAME_SUFFIX = "ReplicaStore";

    protected abstract ReplicaStore delegate();

    @Override
    public void init() throws CacheException {
        delegate().init();
    }

    @Override
    public Set<diskCacheV111.util.PnfsId> index(IndexOption... options) throws CacheException {
        return delegate().index(options);
    }

    @Override
    public ReplicaRecord get(PnfsId id) throws CacheException {
        return delegate().get(id);
    }

    @Override
    public ReplicaRecord create(PnfsId id, Set<? extends OpenOption> flags)
          throws DuplicateEntryException, CacheException {
        return delegate().create(id, flags);
    }

    @Override
    public void remove(PnfsId id) throws CacheException {
        delegate().remove(id);
    }

    @Override
    public FileStoreState isOk() {
        return delegate().isOk();
    }

    @Override
    public void close() {
        delegate().close();
    }

    @Override
    public long getFreeSpace() {
        return delegate().getFreeSpace();
    }

    @Override
    public long getTotalSpace() {
        return delegate().getTotalSpace();
    }

    private String name() {
        String name = getClass().getSimpleName();
        return name.endsWith(NAME_SUFFIX)
              ? name.substring(0, name.length() - NAME_SUFFIX.length())
              : name;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(name());

        String innerName = delegate().toString();
        boolean isInBrackets = Strings.isInBrackets(innerName);

        if (!isInBrackets) {
            sb.append('[');
        }
        sb.append(innerName);
        if (!isInBrackets) {
            sb.append(']');
        }

        return sb.toString();
    }
}
