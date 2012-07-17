package org.dcache.xrootd.pool;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates an open file for reading in the xrootd data server.
 */
public class ReadDescriptor implements FileDescriptor
{
    private final static Logger _log =
        LoggerFactory.getLogger(ReadDescriptor.class);

    /**
     * Update mover meta-information
     */
    private XrootdProtocol_3 _mover;

    public ReadDescriptor(XrootdProtocol_3 mover)
    {
        _mover = mover;
    }

    @Override
    public void close()
    {
        _mover.close(this);
    }

    @Override
    public Reader read(ReadRequest msg)
    {
        return new RegularReader(msg.getStreamId(),
                                 msg.getReadOffset(), msg.bytesToRead(),
                                 this);
    }

    @Override
    public void sync(SyncRequest msg)
    {
        _mover.updateLastTransferred();

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
    public RepositoryChannel getChannel() throws ClosedChannelException
    {
        return _mover.getChannel();
    }

    public XrootdProtocol_3 getMover()
    {
        return _mover;
    }
}

