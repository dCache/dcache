package org.dcache.xrootd.pool;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.dcache.pool.movers.MoverChannel;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;

/**
 * Encapsulates an open file for writing in the xrootd data server.
 */
public class WriteDescriptor extends ReadDescriptor
{
    public WriteDescriptor(MoverChannel<XrootdProtocolInfo> channel)
    {
        super(channel);
    }

    @Override
    public void sync(SyncRequest msg)
        throws IOException
    {
        _channel.sync();
    }

    @Override
    public void write(WriteRequest msg)
        throws IOException
    {
        long position = msg.getWriteOffset();
        for (ByteBuffer buffer: msg.toByteBuffers()) {
            while (buffer.hasRemaining()) {
                position += _channel.write(buffer, position);
            }
        }
    }
}
