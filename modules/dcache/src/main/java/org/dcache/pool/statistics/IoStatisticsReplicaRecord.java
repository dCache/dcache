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

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.HashSet;
import java.util.Set;

import org.dcache.pool.repository.ForwardingReplicaRecord;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.RepositoryChannel;

import static java.util.Objects.requireNonNull;

/**
 * A ReplicaRecord that delegates all activity to some inner ReplicaRecord while
 * offering the possibility to monitor IO performance.
 */
public class IoStatisticsReplicaRecord extends ForwardingReplicaRecord
{
    private final ReplicaRecord inner;

    public enum OpenFlags implements OpenOption
    {
        /**
         * Specifying this flag results in the channel collecting IO statistics.
         */
        ENABLE_IO_STATISTICS,
    }

    public IoStatisticsReplicaRecord(ReplicaRecord inner)
    {
        this.inner = requireNonNull(inner);
    }

    @Override
    public ReplicaRecord delegate()
    {
        return inner;
    }

    @Override
    public synchronized RepositoryChannel openChannel(Set<? extends OpenOption> mode)
            throws IOException
    {
        if (mode.contains(OpenFlags.ENABLE_IO_STATISTICS)) {
            mode = new HashSet<>(mode);
            mode.remove(OpenFlags.ENABLE_IO_STATISTICS);
            return new IoStatisticsChannel(super.openChannel(mode));
        } else {
            return super.openChannel(mode);
        }
    }
}
