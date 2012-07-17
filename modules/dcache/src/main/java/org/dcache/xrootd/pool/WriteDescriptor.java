package org.dcache.xrootd.pool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates an open file for writing in the xrootd data server.
 */
public class WriteDescriptor implements FileDescriptor
{
    private final static Logger _log =
        LoggerFactory.getLogger(WriteDescriptor.class);

    private XrootdProtocol_3 _mover;

    public WriteDescriptor(XrootdProtocol_3 mover)
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
        throws IOException
    {
        _mover.updateLastTransferred();
        _mover.getChannel().sync();
    }

    @Override
    public void write(WriteRequest msg)
        throws IOException
    {
        _mover.preallocate(msg.getWriteOffset() + msg.getDataLength());
        _mover.updateLastTransferred();
        _mover.addTransferredBytes(msg.getDataLength());
        _mover.setWasChanged(true);

        long position = msg.getWriteOffset();
        for (ByteBuffer buffer: msg.toByteBuffers()) {
            while (buffer.hasRemaining()) {
                position += _mover.getChannel().write(buffer, position);
            }
        }
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
