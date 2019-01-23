/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.repository.checksums;

import java.nio.file.OpenOption;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.classic.ChecksumModuleV1;
import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.ForwardingReplicaStore;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.ReplicaStore;

/**
 * This class wraps some existing ReplicaStore and adds support for on-the-fly
 * checksum calculation.
 */
public class ChecksumReplicaStore extends ForwardingReplicaStore
{
    private final ReplicaStore inner;
    private final ChecksumModuleV1 csm;

    public ChecksumReplicaStore(ReplicaStore inner, ChecksumModuleV1 csm)
    {
        this.inner = inner;
        this.csm = csm;
    }

    @Override
    protected ReplicaStore delegate()
    {
        return inner;
    }

    @Override
    public ReplicaRecord create(PnfsId id, Set<? extends OpenOption> flags)
            throws DuplicateEntryException, CacheException
    {
        return new ChecksumReplicaRecord(super.create(id, flags), csm.getDefaultChecksumTypes());
    }
}
