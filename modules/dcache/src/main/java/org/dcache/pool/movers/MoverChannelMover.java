/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 - 2015 Deutsches Elektronen-Synchrotron
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

import java.io.IOException;

import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellPath;

import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.repository.ReplicaDescriptor;

import static com.google.common.base.Preconditions.checkState;

/**
 * A Mover implementation based on the MoverChannel class.
 */
public abstract class MoverChannelMover<P extends ProtocolInfo, M extends MoverChannelMover<P, M>> extends AbstractMover<P, M>
{
    private volatile MoverChannel<P> _wrappedChannel;
    private final MoverChannel.AllocatorMode _allocatorMode;

    public MoverChannelMover(ReplicaDescriptor handle, PoolIoFileMessage message,
                             CellPath pathToDoor,
                             TransferService<M> transferService,
                             MoverChannel.AllocatorMode allocatorMode,
                             ChecksumModule checksumModule)
    {
        super(handle, message, pathToDoor, transferService, checksumModule);
        _allocatorMode = allocatorMode;
    }

    @Override
    public long getTransferTime()
    {
        MoverChannel<P> channel = _wrappedChannel;
        return (channel == null) ? 0 : channel.getTransferTime();
    }

    @Override
    public long getBytesTransferred()
    {
        MoverChannel<P> channel = _wrappedChannel;
        return (channel == null) ? 0 : channel.getBytesTransferred();
    }

    @Override
    public long getLastTransferred()
    {
        MoverChannel<P> channel = _wrappedChannel;
        return (channel == null) ? 0 : channel.getLastTransferred();
    }

    /**
     * Opens a MoverChannel for the replica of this mover.
     *
     * The caller is responsible for closing the channel.
     *
     * The channel will be registered with the mover and will provide information
     * about the progress of the transfer.
     *
     * @return an open MoverChannel
     * @throws DiskErrorCacheException if the file could not be opened
     * @throws IllegalStateException if called more than once
     */
    public synchronized MoverChannel<P> open() throws DiskErrorCacheException
    {
        checkState(_wrappedChannel == null);
        _wrappedChannel = new MoverChannel<>(this, openChannel(), _allocatorMode);
        return _wrappedChannel;
    }

    /**
     * Get the MoverChannel for the replica of this mover.
     *
     * @return an open MoverChannel
     * @throws IllegalStateException if channel is not opened
     */
    public MoverChannel<P> getMoverChannel() {
        MoverChannel<P> channel = _wrappedChannel;
        checkState(channel != null);
        return channel;
    }

    @Override
    protected String getStatus()
    {
        StringBuilder s = new StringBuilder(_protocolInfo.getProtocol());
        try {
            if (_wrappedChannel != null && getIoMode() == IoMode.WRITE) {
                long size = _wrappedChannel.size();
                s.append(":SU=").append(size);
                s.append(";SA=").append(_wrappedChannel.getAllocated());
            }
        } catch (IOException e) {
        }
        return s.toString();
    }
}
