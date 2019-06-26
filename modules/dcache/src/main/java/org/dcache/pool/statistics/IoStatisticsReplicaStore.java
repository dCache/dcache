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
package org.dcache.pool.statistics;

import java.nio.file.OpenOption;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.ForwardingReplicaStore;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.ReplicaStore;

import static java.util.Objects.requireNonNull;

/**
 * Wrap some existing ReplicaStore and add support for monitoring the inner
 * repository IO performance.
 */
public class IoStatisticsReplicaStore extends ForwardingReplicaStore
{
    private final ReplicaStore inner;

    public IoStatisticsReplicaStore(ReplicaStore inner)
    {
        this.inner = requireNonNull(inner);
    }

    @Override
    protected ReplicaStore delegate()
    {
        return inner;
    }

    @Override
    public ReplicaRecord get(PnfsId id) throws CacheException
    {
        ReplicaRecord record = super.get(id);
        return record == null ? null : new IoStatisticsReplicaRecord(record);
    }

    @Override
    public ReplicaRecord create(PnfsId id, Set<? extends OpenOption> flags)
            throws DuplicateEntryException, CacheException
    {
        return new IoStatisticsReplicaRecord(super.create(id, flags));
    }
}
