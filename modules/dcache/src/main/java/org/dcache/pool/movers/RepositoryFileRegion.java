/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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

import static com.google.common.base.Preconditions.checkArgument;

import diskCacheV111.vehicles.ProtocolInfo;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

/**
 * {@link FileRegion} that can be directly written into the socket by passing local IO buffer.
 */
public class RepositoryFileRegion<P extends ProtocolInfo> extends
      AbstractReferenceCounted implements FileRegion {

    private final NettyTransferService<P>.NettyMoverChannel file;

    private final long offset;

    private final long count;

    private long transferred;

    /**
     * Create a new {@link FileRegion} that have to be written into socket.
     *
     * @param moverChannel The {@link MoverChannel} that represents file in the pool.
     * @param offset       The starting position of this region within the file; must be
     *                     non-negative.
     * @param count        The maximum number of bytes to be transferred; must be non-negative.
     */
    public RepositoryFileRegion(NettyTransferService<P>.NettyMoverChannel moverChannel, long offset,
          long count) {

        checkArgument(offset >= 0L, "Files position can't be negative.");
        checkArgument(count >= 0L, "Count can't be negative.");

        this.file = Objects.requireNonNull(moverChannel, "Mover channel can't be null.");
        this.offset = offset;
        this.count = count;
    }

    @Override
    public long position() {
        return offset;
    }

    @Override
    public long transfered() {
        return transferred();
    }

    @Override
    public long transferred() {
        return transferred;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public long transferTo(WritableByteChannel writableByteChannel, long position)
          throws IOException {

        long count = this.count - position;
        checkArgument(count >= 0L,
              "The position must by within region [0 - " + (this.count - 1) + "]");
        checkArgument(position >= 0L, "Files position can't be negative.");

        if (count == 0) {
            return 0L;
        }
        if (refCnt() == 0) {
            throw new IllegalReferenceCountException(0);
        }

        long written = file.transferTo(this.offset + position, count, writableByteChannel);
        if (written > 0) {
            transferred += written;
        }

        return written;
    }

    @Override
    public FileRegion retain() {
        super.retain();
        return this;
    }

    @Override
    public FileRegion retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public FileRegion touch() {
        return this;
    }

    @Override
    public FileRegion touch(Object o) {
        return this;
    }

    @Override
    protected void deallocate() {
        // dCache takes care of closing the file channel
    }
}
