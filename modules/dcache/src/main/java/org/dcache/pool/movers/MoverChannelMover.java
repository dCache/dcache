/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 - 2019 Deutsches Elektronen-Synchrotron
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

import static com.google.common.base.Preconditions.checkState;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.CellPath;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.StandardOpenOption;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.repository.ReplicaDescriptor;

/**
 * A Mover implementation based on the MoverChannel class.
 */
public abstract class MoverChannelMover<P extends ProtocolInfo, M extends MoverChannelMover<P, M>> extends
      AbstractMover<P, M> {

    private volatile MoverChannel<P> _wrappedChannel;

    public MoverChannelMover(ReplicaDescriptor handle, PoolIoFileMessage message,
          CellPath pathToDoor,
          TransferService<M> transferService) {
        super(handle, message, pathToDoor, transferService);
    }

    @Override
    public long getTransferTime() {
        MoverChannel<P> channel = _wrappedChannel;
        return (channel == null) ? 0 : channel.getTransferTime();
    }

    @Override
    public long getBytesTransferred() {
        MoverChannel<P> channel = _wrappedChannel;
        return (channel == null) ? 0 : channel.getBytesTransferred();
    }

    @Override
    public long getLastTransferred() {
        MoverChannel<P> channel = _wrappedChannel;
        return (channel == null) ? 0 : channel.getLastTransferred();
    }

    /**
     * Opens a MoverChannel for the replica of this mover.
     * <p>
     * The caller is responsible for closing the channel.
     * <p>
     * The channel will be registered with the mover and will provide information about the progress
     * of the transfer.
     *
     * @return an open MoverChannel
     * @throws InterruptedIOException  if the mover was cancelled
     * @throws DiskErrorCacheException if the file could not be opened
     * @throws IllegalStateException   if called more than once
     */
    public synchronized MoverChannel<P> open()
          throws DiskErrorCacheException, InterruptedIOException, CacheException {
        checkState(_wrappedChannel == null);
        _wrappedChannel = new MoverChannel<>(this, openChannel());
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
    protected String getStatus() {
        StringBuilder s = new StringBuilder(_protocolInfo.getProtocol());
        try {
            if (_wrappedChannel != null && getIoMode().contains(StandardOpenOption.WRITE)) {
                long size = _wrappedChannel.size();
                s.append(":SU=").append(size);
            }
        } catch (IOException e) {
        }

        s.append(",cl=[")
              .append(((IpProtocolInfo)getProtocolInfo()).getSocketAddress().getAddress().getHostAddress())
              .append("]");
        return s.toString();
    }
}
