package org.dcache.xrootd2.pool;

import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;

import org.dcache.pool.repository.ReadHandle;

import org.dcache.xrootd2.protocol.messages.ReadRequest;
import org.dcache.xrootd2.protocol.messages.ReadVRequest;
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

    private RandomAccessFile _file;

    public ReadDescriptor(RandomAccessFile file)
    {
        if (file == null) {
            throw new IllegalArgumentException("File must be non-null");
        }
        _file = file;
    }

    private boolean isClosed()
    {
        return _file == null;
    }

    @Override
    public void close()
        throws IllegalStateException
    {
        if (isClosed()) {
            throw new IllegalStateException("File not open");
        }

        _file = null;
    }

    @Override
    public Reader read(ReadRequest msg)
        throws IllegalStateException
    {
        if (isClosed()) {
            throw new IllegalStateException("File not open");
        }
        return new RegularReader(_file.getChannel(), msg.getStreamID(),
                                 msg.getReadOffset(), msg.bytesToRead());
    }

    @Override
    public void sync(SyncRequest msg)
        throws IllegalStateException
    {
        if (isClosed()) {
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
        if (isClosed()) {
            throw new IllegalStateException("File not open");
        }

        throw new IOException("File is read only");
    }

    @Override
    public FileChannel getChannel()
    {
        if (isClosed()) {
            throw new IllegalStateException("File not open");
        }

        return _file.getChannel();
    }
}

