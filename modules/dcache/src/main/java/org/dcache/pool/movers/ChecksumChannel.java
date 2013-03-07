package org.dcache.pool.movers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;

import diskCacheV111.util.ChecksumFactory;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;

/**
 * A wrapper for RepositoryChannel that computes a digest
 * on the fly during write as long as all writes are
 * sequential.
 */
public class ChecksumChannel implements RepositoryChannel
{
    private final static Logger _log =
            LoggerFactory.getLogger(ChecksumChannel.class);

    /**
     * Inner channel to which all operations are delegated.
     */
    private final RepositoryChannel _channel;

    /**
     * Factory object for creating digests.
     */
    private final ChecksumFactory _checksumFactory;

    /**
     * Digest used for computing the checksum during write.
     */
    private MessageDigest _digest;

    /**
     * Position of digest computation.
     */
    private long _digestPosition;


    public ChecksumChannel(RepositoryChannel inner,
                           ChecksumFactory checksumFactory)
    {
        _channel = inner;
        _checksumFactory = checksumFactory;
        _digest = _checksumFactory.create();
    }

    @Override
    public long position() throws IOException
    {
        return _channel.position();
    }

    @Override
    public RepositoryChannel position(long position) throws IOException
    {
        return _channel.position(position);
    }

    @Override
    public long size() throws IOException
    {
        return _channel.size();
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException
    {
        abortIfOutOfOrder(position);
        int bytes;
        if (_digest != null) {
            ByteBuffer readOnly = buffer.asReadOnlyBuffer();
            bytes = _channel.write(buffer, position);
            update(readOnly, bytes);
        } else {
            bytes = _channel.write(buffer, position);
        }
        return bytes;
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException
    {
        return _channel.read(buffer, position);
    }

    @Override
    public RepositoryChannel truncate(long size) throws IOException
    {
        return _channel.truncate(size);
    }

    @Override
    public void sync() throws SyncFailedException, IOException
    {
        _channel.sync();
    }

    @Override
    public long transferTo(long position, long count,
                           WritableByteChannel target) throws IOException
    {
        return _channel.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position,
                             long count) throws IOException
    {
        _digest = null;
        return _channel.transferFrom(src, position, count);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException
    {
        abortIfOutOfOrder(position());

        long bytes;
        if (_digest != null) {
            ByteBuffer[] readOnly = new ByteBuffer[srcs.length];
            for (int i = offset; i < offset + length; i++) {
                readOnly[i] = srcs[i].asReadOnlyBuffer();
            }

            bytes = _channel.write(srcs, offset, length);

            long remaining = bytes;
            for (int i = offset; i < offset + length && remaining > 0; i++) {
                remaining -= update(readOnly[i], remaining);
            }
        } else {
            bytes = _channel.write(srcs, offset, length);
        }

        return bytes;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException
    {
        abortIfOutOfOrder(position());

        long bytes;
        if (_digest != null) {
            ByteBuffer[] readOnly = new ByteBuffer[srcs.length];
            for (int i = 0; i < srcs.length; i++) {
                readOnly[i] = srcs[i].asReadOnlyBuffer();
            }

            bytes = _channel.write(srcs);

            long remaining = bytes;
            for (int i = 0; i < readOnly.length && remaining > 0; i++) {
                remaining -= update(readOnly[i], remaining);
            }
        } else {
            bytes = _channel.write(srcs);
        }

        return bytes;
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        abortIfOutOfOrder(position());

        int bytes;
        if (_digest != null) {
            ByteBuffer readOnly = src.asReadOnlyBuffer();
            bytes = _channel.write(src);
            update(readOnly, bytes);
        } else {
            bytes = _channel.write(src);
        }
        return bytes;
    }

    @Override
    public boolean isOpen()
    {
        return _channel.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        _channel.close();
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
            throws IOException
    {
        return _channel.read(dsts, offset, length);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException
    {
        return _channel.read(dsts);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        return _channel.read(dst);
    }

    /**
     * Returns the computed digest or null if the file was not written
     * sequentially.
     */
    public Checksum getChecksum()
    {
        return (_digest == null) ? null : _checksumFactory.create(_digest.digest());
    }

    private void abortIfOutOfOrder(long position)
    {
        if (position != _digestPosition) {
            _digest = null;
            _log.trace("On-transfer checksum not computed due to out-of-order upload");
        }
    }

    private long update(ByteBuffer buffer, long bytes)
    {
        if (bytes < buffer.remaining()) {
            buffer.limit(buffer.position() + (int) bytes);
        }
        long actual = buffer.remaining();
        _digest.update(buffer);
        _digestPosition += actual;
        return actual;
    }
}
