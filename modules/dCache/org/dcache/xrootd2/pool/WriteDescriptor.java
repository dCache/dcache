package org.dcache.xrootd2.pool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ClosedChannelException;

import org.dcache.xrootd2.protocol.messages.ReadRequest;
import org.dcache.xrootd2.protocol.messages.WriteRequest;
import org.dcache.xrootd2.protocol.messages.SyncRequest;

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
        return new RegularReader(msg.getStreamID(),
                                 msg.getReadOffset(), msg.bytesToRead(),
                                 this);
    }

    @Override
    public void sync(SyncRequest msg)
        throws IOException
    {
        _mover.updateLastTransferred();
        _mover.getFile().getFD().sync();
    }

    @Override
    public void write(WriteRequest msg)
        throws IOException
    {
        _mover.preallocate(msg.getWriteOffset() + msg.getDataLength());
        _mover.updateLastTransferred();
        _mover.addTransferredBytes(msg.getDataLength());
        _mover.setWasChanged(true);

        FileChannel channel = _mover.getFile().getChannel();
        long position = msg.getWriteOffset();
        for (ByteBuffer buffer: msg.toByteBuffers()) {
            while (buffer.hasRemaining()) {
                position += channel.write(buffer, position);
            }
        }
    }

    @Override
    public FileChannel getChannel() throws ClosedChannelException
    {
        return _mover.getFile().getChannel();
    }

    public XrootdProtocol_3 getMover()
    {
        return _mover;
    }
}
