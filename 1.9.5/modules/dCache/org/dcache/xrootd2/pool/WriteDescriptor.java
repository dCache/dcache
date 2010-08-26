package org.dcache.xrootd2.pool;

import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.SyncFailedException;
import java.nio.channels.FileChannel;

import diskCacheV111.util.CacheException;

import org.dcache.pool.repository.WriteHandle;

import org.dcache.xrootd2.protocol.messages.ReadRequest;
import org.dcache.xrootd2.protocol.messages.ReadVRequest;
import org.dcache.xrootd2.protocol.messages.WriteRequest;
import org.dcache.xrootd2.protocol.messages.SyncRequest;

import org.apache.log4j.Logger;

/**
 * Encapsulates an open file for writing in the xrootd data server.
 */
public class WriteDescriptor implements FileDescriptor
{
    private final static Logger _log =
        Logger.getLogger(WriteDescriptor.class);

    private RandomAccessFile _file;

    public WriteDescriptor(RandomAccessFile file)
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
    {
        if (isClosed()) {
            throw new IllegalStateException("File not open");
        }
        return new RegularReader(_file.getChannel(), msg.getStreamID(),
                                 msg.getReadOffset(), msg.bytesToRead());
    }

    @Override
    public void sync(SyncRequest msg)
        throws IOException
    {
        if (isClosed()) {
            throw new IllegalStateException("File not open");
        }

        _file.getFD().sync();
    }

    @Override
    public void write(WriteRequest msg)
        throws IOException
    {
        if (isClosed()) {
            throw new IllegalStateException("File not open");
        }

        FileChannel channel = _file.getChannel();
        channel.position(msg.getWriteOffset());
        msg.getData(channel);
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
