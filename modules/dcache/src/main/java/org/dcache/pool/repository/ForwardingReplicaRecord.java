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

import java.io.IOException;
import java.net.URI;
import java.nio.file.OpenOption;
import java.util.Collection;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.vehicles.FileAttributes;

/**
 * A ReplicaRecord that forwards all activity to some delegate ReplicaRecord.
 */
public abstract class ForwardingReplicaRecord implements ReplicaRecord
{
    abstract protected ReplicaRecord delegate();

    @Override
    public PnfsId getPnfsId()
    {
        return delegate().getPnfsId();
    }

    @Override
    public long getReplicaSize()
    {
        return delegate().getReplicaSize();
    }

    @Override
    public FileAttributes getFileAttributes() throws CacheException
    {
        return delegate().getFileAttributes();
    }

    @Override
    public ReplicaState getState()
    {
        return delegate().getState();
    }

    @Override
    public URI getReplicaUri()
    {
        return delegate().getReplicaUri();
    }

    @Override
    public RepositoryChannel openChannel(Set<? extends OpenOption> mode) throws IOException
    {
        return delegate().openChannel(mode);
    }

    @Override
    public long getCreationTime()
    {
        return delegate().getCreationTime();
    }

    @Override
    public long getLastAccessTime()
    {
        return delegate().getLastAccessTime();
    }

    @Override
    public void setLastAccessTime(long time) throws CacheException
    {
        delegate().getLastAccessTime();
    }

    @Override
    public int decrementLinkCount()
    {
        return delegate().decrementLinkCount();
    }

    @Override
    public int incrementLinkCount()
    {
        return delegate().incrementLinkCount();
    }

    @Override
    public int getLinkCount()
    {
        return delegate().getLinkCount();
    }

    @Override
    public boolean isSticky()
    {
        return delegate().isSticky();
    }

    @Override
    public Collection<StickyRecord> removeExpiredStickyFlags() throws CacheException
    {
        return delegate().removeExpiredStickyFlags();
    }

    @Override
    public Collection<StickyRecord> stickyRecords()
    {
        return delegate().stickyRecords();
    }

    @Override
    public <T> T update(Update<T> update) throws CacheException
    {
        return delegate().update(update);
    }
}
