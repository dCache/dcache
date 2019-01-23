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

import org.dcache.pool.movers.ChecksumChannel;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.HashSet;
import java.util.Set;

import org.dcache.pool.repository.ForwardingReplicaRecord;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.ChecksumType;

/**
 * Wrap some existing ReplicaRecord and add support for optionally wrapping
 * a RepositoryChannel with a ChecksumChannel.
 */
public class ChecksumReplicaRecord extends ForwardingReplicaRecord
{
    private final ReplicaRecord inner;
    private final Set<ChecksumType> defaultTypes;

    public enum OpenFlags implements OpenOption
    {
        /**
         * Specifying this flag results in the ReplicaRecord being wrapped by
         * a ChecksumChannel.  This ChecksumChannel is available via the
         * {@literal RepositoryChannel#optionallyAs} method.
         */
        ENABLE_CHECKSUM_CALCULATION;
    }

    public ChecksumReplicaRecord(ReplicaRecord inner, Set<ChecksumType> defaultTypes)
    {
        this.inner = inner;
        this.defaultTypes = defaultTypes;
    }

    @Override
    protected ReplicaRecord delegate()
    {
        return inner;
    }

    @Override
    public synchronized RepositoryChannel openChannel(Set<? extends OpenOption> mode)
            throws IOException
    {
        if (mode.contains(OpenFlags.ENABLE_CHECKSUM_CALCULATION)) {
            Set<? extends OpenOption> innerMode = new HashSet<>(mode);
            innerMode.remove(OpenFlags.ENABLE_CHECKSUM_CALCULATION);
            return new ChecksumChannel(super.openChannel(innerMode), defaultTypes);
        } else {
            return super.openChannel(mode);
        }
    }
}
