package org.dcache.http;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.dcache.pool.repository.RepositortyChannel;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.stream.ChunkedInput;

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
public class ReusableChunkedNioFile implements ChunkedInput
{
    private final RepositortyChannel _channel;
    private final long _endOffset;
    private final int _chunkSize;
    private final HttpProtocol_2 _mover;
    private volatile long _offset;

    public ReusableChunkedNioFile(RepositortyChannel channel,
                                  long offset,
                                  long length,
                                  int chunkSize,
                                  HttpProtocol_2 mover) throws IOException {
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
        _mover = mover;
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
    public boolean hasNextChunk() throws Exception {
        return _offset < _endOffset && _channel.isOpen();
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        return !hasNextChunk();
    }

    /**
     * Method that is mostly copied from ChunkedNioFile#nextChunk but updates
     * the mover information (last transferred, bytes transferred) and uses
     * absolute position reading to avoid problems with multithreaded access.
     *
     * @return ChannelBuffer containing the read bytes
     */
    @Override
    public Object nextChunk() throws Exception {
        long offset = _offset;

        if (_offset >= _endOffset) {
            return null;
        }

        int chunkSize = (int) Math.min(_chunkSize, _endOffset - offset);
        byte [] chunkArray = new byte[chunkSize];

        ByteBuffer chunk = ByteBuffer.wrap(chunkArray);
        int readBytes = 0;

        for (;;) {
            /* use call that does not change the channel's position */
            int localReadBytes = _channel.read(chunk, _offset);

            if (localReadBytes < 0) {
                break;
            }

            readBytes += localReadBytes;
            _mover.updateBytesTransferred(readBytes);

            if (readBytes == chunkSize) {
                break;
            }
        }

        _offset += readBytes;

        _mover.updateLastTransferred();
        return ChannelBuffers.wrappedBuffer(chunkArray);
    }

}
