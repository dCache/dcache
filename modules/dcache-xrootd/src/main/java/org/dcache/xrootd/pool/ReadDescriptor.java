package org.dcache.xrootd.pool;

import java.io.IOException;

import org.dcache.pool.movers.MoverChannel;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;

/**
 * Encapsulates an open file for reading in the xrootd data server.
 */
public class ReadDescriptor implements FileDescriptor
{
    /**
     * Update mover meta-information
     */
    private MoverChannel<XrootdProtocolInfo> _channel;

    public ReadDescriptor(MoverChannel<XrootdProtocolInfo> channel)
    {
        _channel = channel;
    }

    @Override
    public Reader read(ReadRequest msg)
        throws IllegalStateException
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
        throws IllegalStateException
    {
        if (!_channel.isOpen()) {
            throw new IllegalStateException("File not open");
        }

        /* As this is a read only file, there is no reason to sync
         * anything.
         */
    }

    @Override
    public void write(WriteRequest msg)
        throws IOException
    {
        if (!_channel.isOpen()) {
            throw new IllegalStateException("File not open");
        }

        throw new IOException("File is read only");
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

