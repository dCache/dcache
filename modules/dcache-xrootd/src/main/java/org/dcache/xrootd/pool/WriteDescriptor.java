package org.dcache.xrootd.pool;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.dcache.pool.movers.MoverChannel;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;

/**
 * Encapsulates an open file for writing in the xrootd data server.
 */
public class WriteDescriptor implements FileDescriptor
{
    private MoverChannel<XrootdProtocolInfo> _channel;

    public WriteDescriptor(MoverChannel<XrootdProtocolInfo> channel)
    {
        _channel = channel;
    }

    @Override
    public Reader read(ReadRequest msg)
    {
        if (!_channel.isOpen()) {
            throw new IllegalStateException("File not open");
        }

        return new RegularReader(msg.getStreamId(),
                                 msg.getReadOffset(), msg.bytesToRead(),
                                 this);
    }

    @Override
    public void sync(SyncRequest msg)
        throws IOException
    {
        if (!_channel.isOpen()) {
            throw new IllegalStateException("File not open");
        }

        _channel.sync();
    }

    @Override
    public void write(WriteRequest msg)
        throws IOException
    {
        if (!_channel.isOpen()) {
            throw new IllegalStateException("File not open");
        }

        long position = msg.getWriteOffset();
        for (ByteBuffer buffer: msg.toByteBuffers()) {
            while (buffer.hasRemaining()) {
                position += _channel.write(buffer, position);
            }
        }
    }

    @Override
    public MoverChannel<XrootdProtocolInfo> getChannel()
    {
        if (!_channel.isOpen()) {
            throw new IllegalStateException("File not open");
        }

        return _channel;
    }
}
