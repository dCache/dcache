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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.stream.Stream;

import diskCacheV111.util.PnfsId;

import static org.dcache.util.ByteUnit.MiB;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * An implementation of RepositoryChannel that takes care of space
 * allocation. Space must be allocated before it is consumed on the
 * disk. Any over allocation will be adjusted on {@link #close}.
 */
public class AllocatorAwareRepositoryChannel extends ForwardingRepositoryChannel {

    private final Logger LOGGER = LoggerFactory.getLogger(AllocatorAwareRepositoryChannel.class);

    private final RepositoryChannel inner;
    private final Allocator allocator;
    private long allocated;
    private PnfsId id;

    /**
     * The minimum number of bytes to increment the space allocation.
     */
    static final long SPACE_INC = MiB.toBytes(50);

    /**
     * synchronization object used by allocator
     */
    private final Object allocationLock = new Object();

    private final Object positionLock = new Object();

    public AllocatorAwareRepositoryChannel(RepositoryChannel inner, PnfsId id, Allocator allocator) throws IOException {
        this.inner = inner;
        this.allocator = allocator;
        // file existing in the repository already have allocated space.
        this.allocated = inner.size();
        this.id = requireNonNull(id);
    }

    @Override
    protected RepositoryChannel delegate() {
        return inner;
    }

    @Override
    public synchronized void close() throws IOException {

        synchronized (allocationLock) {
            long length = size();
            LOGGER.debug("Adjusting allocation: allocated: {}, file size: {}",
                    allocated, length);
            if (length > allocated) {
                LOGGER.error("BUG detected! Under allocation detected: expected {}, current: {}.", length, allocated);
                try {
                    allocator.allocate(id, length - allocated);
                } catch (InterruptedException e) {
                    /*
                     * Space allocation is broken now. The entry size
                     * matches up with what was actually allocated,
                     * however the file on disk is too large.
                     *
                     * Should only happen during shutdown, so no harm done.
                     */
                    LOGGER.warn("Failed to adjust space reservation because "
                            + "the operation was interrupted. The pool is now over allocated.");
                    Thread.currentThread().interrupt();
                }
            } else if (length < allocated) {
                allocator.free(id, allocated - length);
            }
        }
        super.close();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        synchronized(positionLock) {
            return super.read(dst);
        }
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        synchronized(positionLock) {
            return super.read(dsts);
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        synchronized(positionLock) {
            return delegate().read(dsts, offset, length);
        }
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return this.write(srcs, 0, srcs.length);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {

        long byteToWrite = Stream.of(srcs)
                .skip(offset)
                .mapToLong(Buffer::remaining)
                .limit(length)
                .sum();

        synchronized (positionLock) {
            preallocate(position() + byteToWrite);
            return super.write(srcs, offset, length);
        }
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        synchronized(positionLock) {
            preallocate(size);
            return super.truncate(size);
        }
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        synchronized (positionLock) {
            preallocate(newPosition);
            return super.position(newPosition);
        }
    }

    @Override
    public long position() throws IOException {
        synchronized (positionLock) {
            return super.position();
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        synchronized (positionLock) {
            preallocate(position() + src.remaining());
            return super.write(src);
        }
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        preallocate(position + count);
        return super.transferFrom(src, position, count);
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
        preallocate(position + buffer.remaining());
        return super.write(buffer, position);
    }

    private void preallocate(long pos) throws IOException {
        synchronized (allocationLock) {
            try {
                checkArgument(pos >= 0);

                if (pos > allocated) {
                    allocated += allocate(pos - allocated);
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException(e.getMessage());
            } catch (IllegalStateException e) {
                throw new ClosedChannelException();
            }
        }
    }

    private long allocate(long minRequired) throws InterruptedException, OutOfDiskException
    {
        long delta = Math.max(minRequired, SPACE_INC);
        try {
            allocator.allocate(id, delta);
        } catch (OutOfDiskException e) {
            // Try again, but this time with the minimum required.
            delta = minRequired;
            allocator.allocate(id, delta);
        }
        LOGGER.trace("preallocate: {}", delta);
        return delta;
    }
}
