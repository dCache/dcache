package org.dcache.xrootd.pool;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.dcache.pool.movers.NettyTransferService;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;

/**
 * Encapsulates an open file for writing in the xrootd data server.
 */
public class WriteDescriptor extends ReadDescriptor
{
    private boolean posc;

    public WriteDescriptor(NettyTransferService<XrootdProtocolInfo>.NettyMoverChannel channel, boolean posc)
    {
        super(channel);
        this.posc = posc;
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

    @Override
    public boolean isPersistOnSuccessfulClose()
    {
        return posc;
    }
}
