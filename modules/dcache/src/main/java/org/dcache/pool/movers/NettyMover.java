/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013-2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.movers;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellPath;

import org.dcache.pool.classic.TransferService;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;

import static com.google.common.base.Preconditions.checkState;

/**
 * A mover that served by AbstractNettyTransferServices.
 */
public class NettyMover<P extends ProtocolInfo> extends MoverChannelMover<P, NettyMover<P>>
{
    private final ChecksumFactory checksumFactory;
    private final UUID uuid;
    private ChecksumChannel checksumChannel;

    public NettyMover(ReplicaDescriptor handle,
                      PoolIoFileMessage message,
                      CellPath pathToDoor,
                      TransferService<NettyMover<P>> transferService,
                      ChecksumFactory checksumFactory,
                      UUID uuid)
    {
        super(handle, message, pathToDoor, transferService, MoverChannel.AllocatorMode.HARD);
        this.checksumFactory = checksumFactory;
        this.uuid = uuid;
    }

    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public Set<Checksum> getActualChecksums()
    {
        return (checksumChannel == null)
               ? Collections.<Checksum>emptySet()
               : Optional.fromNullable(checksumChannel.getChecksum()).asSet();
    }

    @Override
    public Set<Checksum> getExpectedChecksums()
    {
        return Collections.emptySet();
    }

    @Override
    public synchronized RepositoryChannel openChannel() throws DiskErrorCacheException
    {
        checkState(checksumChannel == null);
        RepositoryChannel channel = super.openChannel();
        try {
            if (getIoMode() == IoMode.WRITE && checksumFactory != null) {
                channel = checksumChannel = new ChecksumChannel(channel, checksumFactory);
            }
        } catch (Throwable t) {
            /* This should only happen in case of JVM Errors or if the checksum digest cannot be
             * instantiated (which, barring bugs, should never happen).
             */
            try {
                channel.close();
            } catch (IOException e) {
                t.addSuppressed(e);
            }
            Throwables.propagate(t);
        }
        return channel;
    }
}
