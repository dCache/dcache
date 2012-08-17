package org.dcache.xrootd.pool;

import java.nio.ByteBuffer;
import java.util.List;

import java.io.IOException;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.xrootd.protocol.messages.ReadResponse;
import org.dcache.xrootd.protocol.messages.GenericReadRequestMessage.EmbeddedReadRequest;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

/**
 * Reader for kXR_readv requests.
 */
public class VectorReader implements Reader
{
    private final int _id;
    private final List<FileDescriptor> _descriptors;
    private final EmbeddedReadRequest[] _requests;

    private int _index;

    public VectorReader(int id,
                        List<FileDescriptor> descriptors,
                        EmbeddedReadRequest[] requests)
    {
        _id = id;
        _descriptors = descriptors;
        _requests = requests;
        _index = 0;
    }

    public int getStreamID()
    {
        return _id;
    }

    private int getLengthOfRequest(EmbeddedReadRequest request)
            throws IOException
    {
        FileDescriptor descriptor = _descriptors.get(request.getFileHandle());
        RepositoryChannel channel = descriptor.getChannel();
        return (int) Math.min(request.BytesToRead(), channel.size() - request.getOffset());
    }

    private int getChunksInNextFrame(int maxFrameSize) throws IOException
    {
        long length = 0;
        int count = 0;
        for (int i = _index; i < _requests.length && length < maxFrameSize; i++) {
            length += ReadResponse.READ_LIST_HEADER_SIZE;
            length += getLengthOfRequest(_requests[i]);
            count++;
        }
        if (length > maxFrameSize) {
            count--;
        }
        if (count == 0) {
            throw new IllegalStateException("Maximum chunk size exceeded");
        }
        return count;
    }

    private ChannelBuffer readBlock(EmbeddedReadRequest request)
        throws IOException
    {
        FileDescriptor descriptor = _descriptors.get(request.getFileHandle());
        RepositoryChannel channel = descriptor.getChannel();

        long position = request.getOffset();
        int remaining = request.BytesToRead();
        byte[] chunkArray = new byte[remaining];
        ByteBuffer chunk = ByteBuffer.wrap(chunkArray);

        while (remaining > 0) {
            /* use position independent thread safe call */
            int bytes = channel.read(chunk, position);
            if (bytes < 0) {
                break;
            }

            position += bytes;
            remaining -= bytes;
        }

        /* inform the mover about transferred bytes and update last
         * transfer information */
        XrootdProtocol_3 mover = descriptor.getMover();
        mover.addTransferredBytes(chunkArray.length - remaining);
        mover.updateLastTransferred();

        return wrappedBuffer(chunkArray, 0, chunkArray.length - remaining);
    }

    /**
     * Returns the next response message for this read
     * request. Returns null if all data has been read.
     */
    @Override
    public ReadResponse read(int maxFrameSize)
        throws IOException
    {
        if (_index == _requests.length) {
            return null;
        }

        int count = getChunksInNextFrame(maxFrameSize);
        ChannelBuffer[] chunks = new ChannelBuffer[_requests.length];
        for (int i = _index; i < _index + count; i++) {
            chunks[i] = readBlock(_requests[i]);
        }

        VectorReadResponse response = new VectorReadResponse(_id);
        response.write(_requests, chunks, _index, count);
        response.setIncomplete(_index + count < _requests.length);
        _index += count;

        return response;
    }

    /**
     * Specialized response for vector read requests.
     *
     * In contrast to ReadResponse, VectorReadResponse supports zero-copy
     * appending the response data.
     */
    public static class VectorReadResponse extends ReadResponse
    {
        public VectorReadResponse(int sId) {
            super(sId, 0);
        }

        private ChannelBuffer getReadListHeader(EmbeddedReadRequest request, int actualLength)
        {
            ChannelBuffer buffer = ChannelBuffers.buffer(16);
            buffer.writeInt(request.getFileHandle());
            buffer.writeInt(actualLength);
            buffer.writeLong(request.getOffset());
            return buffer;
        }

        public void write(EmbeddedReadRequest[] requests,
                          ChannelBuffer[] buffers,
                          int offset, int length)
        {
            ChannelBuffer[] reply = new ChannelBuffer[2 * length + 1];
            reply[0] = _buffer;
            for (int i = 0; i < length; i++) {
                reply[2 * i + 1] = getReadListHeader(requests[offset + i], buffers[offset + i].readableBytes());
                reply[2 * i + 2] = buffers[offset + i];
            }
            _buffer = wrappedBuffer(reply);
        }
    }
}
