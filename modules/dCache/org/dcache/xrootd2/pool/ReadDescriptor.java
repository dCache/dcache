package org.dcache.xrootd2.pool;

import java.io.IOException;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.xrootd2.protocol.messages.ReadRequest;
import org.dcache.xrootd2.protocol.messages.WriteRequest;
import org.dcache.xrootd2.protocol.messages.SyncRequest;

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
        throws IllegalStateException
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
        throws IllegalStateException
    {
        if (isMoverShutdown()) {
            throw new IllegalStateException("File not open");
        }

        _mover.updateLastTransferred();

        /* As this is a read only file, there is no reason to sync
         * anything.
         */
    }

    @Override
    public void write(WriteRequest msg)
        throws IOException
    {
        if (isMoverShutdown()) {
            throw new IllegalStateException("File not open");
        }

        throw new IOException("File is read only");
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

