package org.dcache.xrootd2.pool;

import java.nio.channels.FileChannel;
import java.io.IOException;

import org.dcache.xrootd2.protocol.messages.ReadResponse;

/**
 * Reader for kXR_read requests.
 */
public class RegularReader implements Reader
{
    private final static int CHUNK_SIZE = 65536;

    private final FileChannel _channel;
    private final int _id;
    private long _position;
    private int _length;

    public RegularReader(FileChannel channel, int id, long position, int length)
    {
        _channel = channel;
        _id = id;
        _position = position;
        _length = length;
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

        int length = Math.min(_length, maxFrameSize);
        ReadResponse response = new ReadResponse(_id, length);
        _channel.position(_position);
        length = response.writeBytes(_channel, length);
        _position += length;
        _length -= length;
        response.setIncomplete(_length != 0);

        return response;
    }
}
