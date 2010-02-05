package org.dcache.xrootd2.pool;

import java.nio.channels.FileChannel;
import java.io.IOException;

import java.util.List;

import org.dcache.xrootd2.protocol.messages.ReadResponse;
import org.dcache.xrootd2.protocol.messages.GenericReadRequestMessage.EmbeddedReadRequest;

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

    private int getSizeOfNextFrame(int maxFrameSize)
    {
        int length = 0;
        for (int i = _index; i < _requests.length; i++) {
            EmbeddedReadRequest req = _requests[_index];
            int sizeOfNextBlock =
                req.BytesToRead() + ReadResponse.READ_LIST_HEADER_SIZE;
            if (length + sizeOfNextBlock > maxFrameSize) {
                break;
            }
            length += sizeOfNextBlock;
        }

        if (length == 0) {
            throw new IllegalStateException("Maximum chunk size exceeded");
        }

        return length;
    }

    private int readBlock(ReadResponse response, EmbeddedReadRequest request)
        throws IOException
    {
        FileDescriptor descriptor = _descriptors.get(request.getFileHandle());
        FileChannel channel = descriptor.getChannel();
        long position = request.getOffset();
        long end = position + request.BytesToRead();

        int length = response.writeBytes(request);
        channel.position(position);
        while (position < end) {
            int read = response.writeBytes(channel, (int) (end - position));
            position += read;
            length += read;
        }

        return length;
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

        int length = getSizeOfNextFrame(maxFrameSize);
        ReadResponse response = new ReadResponse(_id, length);
        while (length > 0 && _index < _requests.length) {
            length -= readBlock(response, _requests[_index]);
            _index = _index + 1;
        }

        response.setIncomplete(_index < _requests.length);

        return response;
    }
}
