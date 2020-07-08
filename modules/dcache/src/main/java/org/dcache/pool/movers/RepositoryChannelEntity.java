/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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

import org.apache.http.entity.AbstractHttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

import dmg.util.Exceptions;

import org.dcache.pool.repository.RepositoryChannel;

/**
 * An HttpEntity based on repository channel.  This is broadly similar
 * to Apache's InputStreamEntity with the following differences:
 * <ul>
 * <li>Objects are backed by RepositoryChannel objects, rather than an InputStream</li>
 * <li>The supplied RepositoryChannel is never closed; for example, the
 * {@literal getContent} method returns an InputStream where the {@literal close}
 * method does not do anything.</li>
 * <li>The Entity is repeatable.</li>
 * <li>In the absence of any errors, this HttpEntity knows the file's size.  If
 * the repository cannot determine the file's size then a chunked encoded
 * transfer will be used.</li>
 * <ul>
 */
public class RepositoryChannelEntity extends AbstractHttpEntity
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryChannelEntity.class);

    private final RepositoryChannel channel;

    public RepositoryChannelEntity(RepositoryChannel channel)
    {
        this.channel = channel;
    }

    @Override
    public boolean isRepeatable()
    {
        return true;
    }

    @Override
    public long getContentLength()
    {
        try {
            return channel.size();
        } catch (IOException e) {
            LOGGER.warn("Failed to discover file size: {}", Exceptions.meaningfulMessage(e));
            return -1; // triggers a chunked encoded transfer.
        }
    }

    @Override
    public InputStream getContent() throws IOException
    {
        final InputStream stream = Channels.newInputStream(channel);

        return new InputStream() {
            @Override
            public int read() throws IOException
            {
                return stream.read();
            }

            @Override
            public int read(byte b[]) throws IOException
            {
                return stream.read(b);
            }

            @Override
            public int read(byte b[], int off, int len) throws IOException
            {
                return stream.read(b, off, len);
            }

            @Override
            public long skip(long n) throws IOException
            {
                return stream.skip(n);
            }

            @Override
            public int available() throws IOException
            {
                return stream.available();
            }

            @Override
            public void close() throws IOException
            {
                // Suppress stream.close(), as this would call channel.close(),
                // which would prevent using this HttpEntity in multiple
                // requests.
            }

            @Override
            public void mark(int readlimit)
            {
                stream.mark(readlimit);
            }

            @Override
            public void reset() throws IOException
            {
                stream.reset();
            }

            @Override
            public boolean markSupported()
            {
                return stream.markSupported();
            }
        };
    }

    @Override
    public void writeTo(OutputStream outstream) throws IOException
    {
        final byte[] buffer = new byte[OUTPUT_BUFFER_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(buffer);

        long offset = 0l;
        int l;
        while ((l = channel.read(bb, offset)) != -1) {
            outstream.write(buffer, 0, l);
            offset += l;
            bb.clear();
        }

        // Suppress calling channel.close() here to allow entity reuse.
    }

    @Override
    public boolean isStreaming()
    {
        return true;
    }
}
