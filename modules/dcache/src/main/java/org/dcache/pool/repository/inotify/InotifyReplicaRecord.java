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


import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.ForwardingReplicaRecord;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.RepositoryChannel;

/**
 * Wrap some existing ReplicaRecord and add support for optionally wrapping
 * a RepositoryChannel with an InotifyChannel.
 */
public class InotifyReplicaRecord extends ForwardingReplicaRecord
{
    private final ReplicaRecord inner;
    private final NotificationAmplifier notification;

    private Duration suppressDuration = Duration.ZERO;

    public enum OpenFlags implements OpenOption
    {
        /**
         * Specifying this flag results in the ReplicaRecord being wrapped by
         * a InotifyChannel, which monitors client activity and generates
         * inotify events if a client is monitoring that file or its parent
         * directory.
         */
        ENABLE_INOTIFY_MONITORING;
    }


    public InotifyReplicaRecord(ReplicaRecord inner, NotificationAmplifier notification,
            PnfsId target)
    {
        this.inner = inner;
        this.notification = notification;
    }

    /**
     * Update the suppression period for all subsequently opened
     * RepositoryChannel. Any already opened channels are not affected.
     */
    public void setSuppressDuration(Duration duration)
    {
        suppressDuration = duration;
    }

    @Override
    protected ReplicaRecord delegate()
    {
        return inner;
    }

    @Override
    public RepositoryChannel openChannel(Set<? extends OpenOption> mode)
            throws IOException
    {
        boolean inotifyRequested = mode.contains(OpenFlags.ENABLE_INOTIFY_MONITORING);

        if (!inotifyRequested) {
            return super.openChannel(mode);
        }

        Set<? extends OpenOption> innerMode = new HashSet<>(mode);
        innerMode.remove(OpenFlags.ENABLE_INOTIFY_MONITORING);
        RepositoryChannel innerChannel = super.openChannel(innerMode);

        boolean openForWrite = mode.contains(StandardOpenOption.WRITE);

        InotifyChannel channel = new InotifyChannel(innerChannel, notification,
                getPnfsId(), openForWrite);
        channel.setSuppressDuration(suppressDuration);
        channel.sendOpenEvent();
        return channel;
    }
}
