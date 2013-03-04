package org.dcache.xrootd.pool;

import java.io.IOException;

import org.dcache.pool.movers.MoverChannel;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.messages.ReadRequest;
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
    private MoverChannel<XrootdProtocolInfo> _channel;

    public ReadDescriptor(MoverChannel<XrootdProtocolInfo> channel)
    {
        _channel = channel;
    }

    @Override
    public Reader read(ReadRequest msg)
    {
        return new RegularReader(msg, this);
    }

    @Override
    public void sync(SyncRequest msg)
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

