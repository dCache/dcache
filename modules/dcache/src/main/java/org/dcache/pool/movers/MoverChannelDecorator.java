/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 - 2017 Deutsches Elektronen-Synchrotron
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
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Set;

import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

public class MoverChannelDecorator<T extends ProtocolInfo> implements RepositoryChannel
{
    private final MoverChannel<T> channel;

    public MoverChannelDecorator(MoverChannel<T> channel)
    {
        this.channel = channel;
    }

    @Override
    public long position() throws IOException
    {
        return channel.position();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        return channel.read(dst);
    }

    public FileAttributes getFileAttributes()
    {
        return channel.getFileAttributes();
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException
    {
        return channel.transferTo(position, count, target);
    }

    @Override
    public MoverChannel<T> position(long position) throws IOException
    {
        return channel.position(position);
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException
    {
        return channel.read(buffer, position);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException
    {
        return channel.transferFrom(src, position, count);
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException
    {
        return channel.write(buffer, position);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException
    {
        return channel.write(srcs);
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    @Override
    public MoverChannel<T> truncate(long size) throws IOException
    {
        return channel.truncate(size);
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
    {
        return channel.write(srcs, offset, length);
    }

    public T getProtocolInfo()
    {
        return channel.getProtocolInfo();
    }

    @Override
    public long size() throws IOException
    {
        return channel.size();
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        return channel.write(src);
    }

    public Set<? extends OpenOption> getIoMode()
    {
        return channel.getIoMode();
    }

    @Override
    public void sync() throws IOException
    {
        channel.sync();
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException
    {
        return channel.read(dsts);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException
    {
        return channel.read(dsts, offset, length);
    }
}
