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
import java.nio.file.AccessDeniedException;

/**
 * A {@link RepositoryChannel} which will reject any write operation
 */
public class ReadonlyRepositoryChannel implements RepositoryChannel {

    private final RepositoryChannel inner;

    public ReadonlyRepositoryChannel(RepositoryChannel inner) {
        this.inner = inner;
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
        throw new AccessDeniedException("Read-only channel");
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        return inner.read(buffer, position);
    }

    @Override
    public void sync() throws SyncFailedException, IOException {
        throw new AccessDeniedException("Read-only channel");
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return inner.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        throw new AccessDeniedException("Read-only channel");
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return inner.read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new AccessDeniedException("Read-only channel");
    }

    @Override
    public long position() throws IOException {
        return inner.position();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        return inner.position(newPosition);
    }

    @Override
    public long size() throws IOException {
        return inner.size();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new AccessDeniedException("Read-only channel");
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        throw new AccessDeniedException("Read-only channel");
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        throw new AccessDeniedException("Read-only channel");
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return inner.read(dsts, offset, length);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return inner.read(dsts);
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }

    @Override
    public String toString() {
        return "read-only channel of " + inner.toString();
    }

}
