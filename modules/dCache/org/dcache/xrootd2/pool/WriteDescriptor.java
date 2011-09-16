package org.dcache.xrootd2.pool;

import java.io.IOException;

import org.dcache.pool.repository.RepositortyChannel;
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

        return new RegularReader(msg.getStreamID(),
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

        RepositortyChannel channel = _mover.getChannel();
        channel.position(msg.getWriteOffset());
        msg.getData(channel);
    }

    @Override
    public RepositortyChannel getChannel()
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
