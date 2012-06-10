package org.dcache.xrootd.pool;

import java.io.IOException;
import java.nio.ByteBuffer;

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
        if (mover.getChannel() == null) {
            throw new IllegalArgumentException("File must be non-null");
        }

        _mover = mover;
    }

    private boolean isMoverShutdown()
    {
        return (_mover == null || _mover.getChannel() == null);
    }

    @Override
    public void close()
    {
        if (isMoverShutdown()) {
            _log.debug("Mover has been closed, possibly due to a timeout.");
        } else {
            _mover.close(this);
        }
    }

    @Override
    public Reader read(ReadRequest msg)
    {
        if (isMoverShutdown()) {
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
        if (isMoverShutdown()) {
            throw new IllegalStateException("File not open");
        }

        _mover.updateLastTransferred();
        _mover.getChannel().sync();
    }

    @Override
    public void write(WriteRequest msg)
        throws IOException, InterruptedException
    {
        if (isMoverShutdown()) {
            throw new IllegalStateException("File not open");
        }

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
    public RepositoryChannel getChannel()
    {
        if (isMoverShutdown()) {
            throw new IllegalStateException("File not open");
        }

        return _mover.getChannel();
    }

    public XrootdProtocol_3 getMover()
    {
        return _mover;
    }
}
