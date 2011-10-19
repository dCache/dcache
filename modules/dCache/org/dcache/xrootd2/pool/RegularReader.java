package org.dcache.xrootd2.pool;

import java.io.IOException;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.xrootd2.protocol.messages.ReadResponse;

/**
 * Reader for kXR_read requests.
 */
public class RegularReader implements Reader
{
    private final int _id;
    private long _position;
    private int _length;
    private FileDescriptor _descriptor;

    public RegularReader(int id, long position, int length,
                         FileDescriptor descriptor)
    {
        _id = id;
        _position = position;
        _length = length;
        _descriptor = descriptor;
    }

    public int getStreamID()
    {
        return _id;
    }

    /**
     * Returns the next response message for this read
     * request. Returns null if all data has been read.
     */
    public ReadResponse read(int maxFrameSize)
        throws IOException
    {
        if (_length == 0) {
            return null;
        }

        RepositoryChannel channel = _descriptor.getChannel();

        int length = Math.min(_length, maxFrameSize);
        ReadResponse response = new ReadResponse(_id, length);
        channel.position(_position);
        length = response.writeBytes(channel, length);
        _position += length;
        _length -= length;
        response.setIncomplete(_length != 0);

        XrootdProtocol_3 mover = _descriptor.getMover();

        mover.addTransferredBytes(length);
        mover.updateLastTransferred();
        return response;
    }
}

