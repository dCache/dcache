package org.dcache.pool.movers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;

import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.pool.repository.ForwardingRepositoryChannel;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

/**
 * A wrapper for RepositoryChannel adding features used by movers.
 */
public class MoverChannel<T extends ProtocolInfo> extends ForwardingRepositoryChannel
{
    private static final Logger _logSpaceAllocation =
        LoggerFactory.getLogger("logger.dev.org.dcache.poolspacemonitor." +
                                MoverChannel.class.getName());

    /**
     * Inner channel to which most operations are delegated.
     */
    private final RepositoryChannel _channel;

    /**
     * The {@link OpenOption} of the mover that created this MoverChannel.
     */
    private final Set<? extends OpenOption> _mode;

    /**
     * Timestamp of when the transfer started.
     */
    private final long _transferStarted =
        System.currentTimeMillis();

    /**
     * Timestamp of when the last block was transferred.
     */
    private final AtomicLong _lastTransferred =
        new AtomicLong(_transferStarted);

    /**
     * The number of bytes transferred.
     */
    private final AtomicLong _bytesTransferred =
        new AtomicLong(0);

    /**
     * ProtocolInfo associated with the transfer.
     */
    private final T _protocolInfo;

    /**
     * The FileAttributes associated with the file being transfered.
     */
    private final FileAttributes _fileAttributes;

    public MoverChannel(Mover<T> mover, RepositoryChannel channel)
    {
        this(mover.getIoMode(), mover.getFileAttributes(), mover.getProtocolInfo(), channel);
    }

    public MoverChannel(Set<? extends OpenOption> mode, FileAttributes attributes, T protocolInfo,
            RepositoryChannel channel)
    {
        _mode = mode;
        _protocolInfo = protocolInfo;
        _channel = channel;
        _fileAttributes = attributes;
    }

    @Override
    protected RepositoryChannel delegate() {
        return _channel;
    }

    @Override
    public synchronized MoverChannel<T> position(long position)
        throws IOException
    {
        _channel.position(position);
        return this;
    }

    @Override
    public synchronized MoverChannel<T> truncate(long size) throws IOException
    {
        try {
            _channel.truncate(size);
            return this;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        _lastTransferred.set(System.currentTimeMillis());
        _channel.close();
    }

    @Override
    public synchronized int read(ByteBuffer dst) throws IOException
    {
        try {
            int bytes = _channel.read(dst);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        try {
            int bytes = _channel.read(buffer, position);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public synchronized long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        try {
            long bytes = _channel.read(dsts, offset, length);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public synchronized long read(ByteBuffer[] dsts) throws IOException {
        try {
            long bytes = _channel.read(dsts);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public synchronized int write(ByteBuffer src) throws IOException {
        try {
            int bytes = _channel.write(src);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
        try {
            int bytes = _channel.write(buffer, position);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public synchronized long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        try {
            long bytes = _channel.write(srcs, offset, length);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public synchronized long write(ByteBuffer[] srcs) throws IOException {
        try {
            long bytes = _channel.write(srcs);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        try {
            long bytes = _channel.transferTo(position, count, target);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        try {
            long bytes = _channel.transferFrom(src, position, count);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    public Set<? extends OpenOption> getIoMode() {
        return _mode;
    }

    public T getProtocolInfo() {
        return _protocolInfo;
    }

    public FileAttributes getFileAttributes() {
        return _fileAttributes;
    }

    public long getBytesTransferred() {
        return _bytesTransferred.get();
    }

    public long getTransferTime() {
        return (_channel.isOpen()
                ? System.currentTimeMillis()
                : getLastTransferred()) - _transferStarted;
    }

    public long getLastTransferred() {
        return _lastTransferred.get();
    }
}
