package org.dcache.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.nio.ByteBuffer;

import org.dcache.pool.repository.RepositoryChannel;

/*
 * Portions of this file based on JBoss Netty ChunkedNioFile which has the
 * following copyright:
 *
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
public class ReusableChunkedNioFile implements ChunkedInput<ByteBuf>
{
    private final RepositoryChannel _channel;
    private final long _endOffset;
    private final int _chunkSize;
    private volatile long _offset;

    public ReusableChunkedNioFile(RepositoryChannel channel,
                                  long offset,
                                  long length,
                                  int chunkSize)
    {
        if (channel == null) {
            throw new NullPointerException("Channel must not be null");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("offset: " + offset +
                                               " (expected: 0 or greater)");
        }

        if (length < 0) {
            throw new IllegalArgumentException("length: " + length +
                                               " (expected: 0 or greater)");
        }

        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize: " + chunkSize +
                                               " (expected: 1 or greater)");
        }

        _channel = channel;
        _chunkSize = chunkSize;
        _offset = offset;
        _endOffset = _offset + length;
    }

    /**
     * With a normal ChunkedNioFile, netty at some point receives a
     * "connection closed by peer" signal and closes the file, trying to
     * release the resources. As this closes the disk-file, the mover becomes
     * useless despite keep-alive. To avoid this, close here is a no-op.
     *
     */
    @Override
    public void close() throws Exception {
        /* make sure to close the backing stream yourself */
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        return _offset >= _endOffset || !_channel.isOpen();
    }

    /**
     * Method that is mostly copied from ChunkedNioFile#nextChunk but updates
     * the mover information (last transferred, bytes transferred) and uses
     * absolute position reading to avoid problems with multithreaded access.
     *
     * @return ChannelBuffer containing the read bytes
     */
    @Override
    public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception
    {
        long offset = _offset;

        if (offset >= _endOffset) {
            return null;
        }

        int length = (int) Math.min(_chunkSize, _endOffset - offset);

        ByteBuf chunk = ctx.alloc().ioBuffer(length);
        boolean release = true;
        try {
            ByteBuffer buffer = chunk.nioBuffer(0, length);

            while (buffer.hasRemaining()) {
                /* use position independent thread safe call */
                int bytes = _channel.read(buffer, offset);
                if (bytes < 0) {
                    break;
                }
                offset += bytes;
            }
            chunk.writerIndex(buffer.position());
            _offset = offset;
            release = false;
            return chunk;
        } finally {
            if (release) {
                chunk.release();
            }
        }
    }

    /**
     * Returns the repository channel. Used for unit testing.
     */
    RepositoryChannel getChannel()
    {
        return _channel;
    }

    /**
     * Returns the end offset. Used for unit testing.
     */
    long getEndOffset() {
        return _endOffset;
    }

    /**
     * Returns the current offset. Used for unit testing.
     */
    long getOffset() {
        return _offset;
    }
}
