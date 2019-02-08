/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.repository.inotify;

import java.nio.file.OpenOption;
import java.time.Duration;
import java.util.Set;

import diskCacheV111.namespace.EventReceiver;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.ForwardingReplicaStore;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.ReplicaStore;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A ReplicaStore that wraps all ReplicaRecord objects in an
 * InotifyReplicaRecord.
 */
public class InotifyReplicaStore extends ForwardingReplicaStore
{
    private final ReplicaStore inner;
    private final NotificationAmplifier notification;
    private final Duration suppression;

    public InotifyReplicaStore(ReplicaStore inner, NotificationAmplifier notication,
            Duration duration)
    {
        checkArgument(!duration.isNegative(), "Negative suppression duration"
                + " not allowed: %s", duration);
        this.inner = requireNonNull(inner);
        this.notification = requireNonNull(notication);
        suppression = duration;
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
        ReplicaRecord innerRecord = super.create(id, flags);
        InotifyReplicaRecord record = new InotifyReplicaRecord(innerRecord, notification, id);
        record.setSuppressDuration(suppression);
        return record;
    }

    @Override
    public ReplicaRecord get(PnfsId id) throws CacheException
    {
        ReplicaRecord innerRecord = super.get(id);
        if (innerRecord == null) {
            return null;
        }

        InotifyReplicaRecord record = new InotifyReplicaRecord(innerRecord, notification, id);
        record.setSuppressDuration(suppression);
        return record;
    }

    @Override
    public String toString()
    {
        return "InotifyMonitoring[" + inner.toString() + "]";
    }
}
