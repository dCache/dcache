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
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;

/**
 * A {@link RepositoryChannel| implementation which forwards all its
 * method calls to another channel. Subclasses should override one or more
 * methods to modify the behavior of the backing file system as desired per the
 * <a href="http://en.wikipedia.org/wiki/Decorator_pattern">decorator
 * pattern</a>.
 */
public abstract class ForwardingRepositoryChannel implements RepositoryChannel {

    /**
     * Returns the backing delegate instance that methods are forwarded to.
     *
     * @return backing delegate instance
     */
    protected abstract RepositoryChannel delegate();

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
        return delegate().write(buffer, position);
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        return delegate().read(buffer, position);
    }

    @Override
    public void sync() throws SyncFailedException, IOException {
        delegate().sync();
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return delegate().transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        return delegate().transferFrom(src, position, count);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return delegate().read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return delegate().write(src);
    }

    @Override
    public long position() throws IOException {
        return delegate().position();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        return delegate().position(newPosition);
    }

    @Override
    public long size() throws IOException {
        return delegate().size();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        return delegate().truncate(size);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return delegate().write(srcs, offset, length);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return delegate().write(srcs);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return delegate().read(dsts, offset, length);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return delegate().read(dsts);
    }

    @Override
    public boolean isOpen() {
        return delegate().isOpen();
    }

    @Override
    public void close() throws IOException {
        delegate().close();
    }

    @Override
    public <T> Optional<T> optionallyAs(Class<T> type)
    {
        if (type.isAssignableFrom(getClass())) {
            return Optional.of(type.cast(this));
        } else {
            return delegate().optionallyAs(type);
        }
    }
}
