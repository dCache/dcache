package org.dcache.xrootd.pool;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.dcache.pool.movers.MoverChannel;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;

/**
 * Encapsulates an open file for reading in the xrootd data server.
 */
public class ReadDescriptor implements FileDescriptor
{
    /**
     * Update mover meta-information
     */
    protected MoverChannel<XrootdProtocolInfo> _channel;

    public ReadDescriptor(MoverChannel<XrootdProtocolInfo> channel)
    {
        _channel = channel;
    }

    @Override
    public void read(ByteBuffer buffer, long position) throws IOException
    {
        while (buffer.hasRemaining()) {
            /* use position independent thread safe call */
            int bytes = _channel.read(buffer, position);
            if (bytes < 0) {
                break;
            }
            position += bytes;
        }
    }

    @Override
    public void sync(SyncRequest msg) throws IOException
    {
        /* As this is a read only file, there is no reason to sync
         * anything.
         */
    }

    @Override
    public void write(WriteRequest msg)
        throws IOException
    {
        throw new IOException("File is read only");
    }

    @Override
    public MoverChannel<XrootdProtocolInfo> getChannel()
    {
        return _channel;
    }
}

