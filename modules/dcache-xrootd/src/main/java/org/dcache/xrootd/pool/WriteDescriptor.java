package org.dcache.xrootd.pool;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.dcache.pool.movers.NettyTransferService;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.messages.OkResponse;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.XrootdResponse;
import org.dcache.xrootd.util.ByteBuffersProvider;

/**
 * Encapsulates an open file for writing in the xrootd data server.
 */
public class WriteDescriptor extends ReadDescriptor
{
    private boolean posc;
    private boolean closed;

    public WriteDescriptor(NettyTransferService<XrootdProtocolInfo>.NettyMoverChannel channel, boolean posc)
    {
        super(channel);
        this.posc = posc;
        closed = false;
    }

    @Override
    public synchronized XrootdResponse<SyncRequest> sync(SyncRequest msg)
        throws IOException, InterruptedException
    {
        if (!closed) {
            _channel.sync();
        }
        return new OkResponse(msg);
    }

    @Override
    public synchronized void write(ByteBuffersProvider provider)
                    throws IOException
    {
        if (!closed) {
            long position = provider.getWriteOffset();
            for (ByteBuffer buffer : provider.toByteBuffers()) {
                while (buffer.hasRemaining()) {
                    position += _channel.write(buffer, position);
                }
            }
        }
    }

    @Override
    public boolean isPersistOnSuccessfulClose()
    {
        return posc;
    }

    public synchronized void close() {
        closed = true;
    }
}
