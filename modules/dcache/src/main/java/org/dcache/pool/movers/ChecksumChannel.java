package org.dcache.pool.movers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import diskCacheV111.util.ChecksumFactory;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;

/**
 * A wrapper for RepositoryChannel that computes a digest
 * on the fly during write as long as all writes are
 * sequential.
 */
public class ChecksumChannel implements RepositoryChannel
{
    private static final Logger _log =
            LoggerFactory.getLogger(ChecksumChannel.class);

    /**
     * Inner channel to which all operations are delegated.
     */
    @VisibleForTesting
    RepositoryChannel _channel;

    /**
     * Factory object for creating digests.
     */
    private final ChecksumFactory _checksumFactory;

    /**
     * Digest used for computing the checksum during write.
     */
    private final MessageDigest _digest;

    /**
     * Cached checksum after getChecksum is called the first time.
     */
    private Checksum _finalChecksum;

    /**
     * RangeSet to keep track of written bytes
     */
    private final RangeSet<Long> _dataRangeSet = TreeRangeSet.create();

    /**
     * Reference to the range containing the file beginning
     */
    private Range<Long> _fileStartRange = Range.openClosed(0L, 0L);

    /**
     * Flag to indicate whether it is still possible to calculated a checksum
     */
    private boolean _isChecksumViable = true;

    /**
     * Flag to indicate whether we still allow writing to the channel.
     * This flag is set to false after getChecksum has been called.
     */
    private boolean _isWritable = true;

    /**
     * lock to prevent writes while or after getChecksum was called
     */
    private final ReadWriteLock _checksumLock = new ReentrantReadWriteLock(false);

    /**
     * Buffer to be used for reading data back from the inner channel for
     * checksum calculations.
     */
    @VisibleForTesting
    ByteBuffer _readBackBuffer = ByteBuffer.allocate(256 * 1024);

    /**
     * Buffer to be used for feeding the checksum digester with 0s to fill up
     * gaps in ranges.
     */
    @VisibleForTesting
    ByteBuffer _zerosBuffer = ByteBuffer.allocate(256 * 1024);

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
        Lock lock = _checksumLock.readLock();
        lock.lock();
        try {
            checkState(_isWritable, "ChecksumChannel must not be written to after getChecksum");

            int bytes;
            if (_isChecksumViable) {
                ByteBuffer readOnly = buffer.asReadOnlyBuffer();
                bytes = _channel.write(buffer, position);
                updateChecksum(readOnly, position, bytes);
            } else {
                bytes = _channel.write(buffer, position);
            }
            return bytes;
        } finally {
            lock.unlock();
        }
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
    public void sync() throws IOException
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
        _isChecksumViable = false;
        return _channel.transferFrom(src, position, count);
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        Lock lock = _checksumLock.readLock();
        lock.lock();
        try {
            checkState(_isWritable, "ChecksumChannel must not be written to after getChecksum");

            int bytes;
            if (_isChecksumViable) {
                bytes = writeWithChecksumUpdate(src);
            } else {
                bytes = _channel.write(src);
            }
            return bytes;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException
    {
        Lock lock = _checksumLock.readLock();
        lock.lock();
        try {
            checkState(_isWritable, "ChecksumChannel must not be written to after getChecksum");

            long bytes = 0;
            if (_isChecksumViable) {
                for (int i = offset; i < offset + length; i++) {
                    bytes += writeWithChecksumUpdate(srcs[i]);
                }
            } else {
                bytes = _channel.write(srcs, offset, length);
            }
            return bytes;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public synchronized long write(ByteBuffer[] srcs) throws IOException
    {
        return write(srcs, 0, srcs.length);
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
     * @return final checksum of this channel
     */
    public Checksum getChecksum()
    {
        if (!_isChecksumViable) {
            return null;
        }

        if (_finalChecksum == null) {
            _finalChecksum = finalizeChecksum();
        }
        return _finalChecksum;
    }

    /**
     * Returns the computed digest or null if overlapping writes have been detected.
     *
     * @return Checksum
     */
    private Checksum finalizeChecksum() {
        Lock lock = _checksumLock.writeLock();
        lock.lock();
        try {
            _isWritable = false;

            if (_dataRangeSet.asRanges().size() != 1 || _fileStartRange.isEmpty()) {
                feedZerosToDigesterForRangeGaps();
            }

            return _checksumFactory.create(_digest.digest());
        } catch (IOException e) {
            _log.info("Unable to generate checksum of sparse file: {}", e.toString());
            return null;
        } finally {
            lock.unlock();
        }
    }

    private void feedZerosToDigesterForRangeGaps() throws IOException {
        ArrayList<Range<Long>> complement = newArrayList(_dataRangeSet.complement().subRangeSet(Range.closed(0L, size())).asRanges());
        complement.sort((r1, r2) -> r1.lowerEndpoint().compareTo(r2.lowerEndpoint()));

        for (Range<Long> range : complement) {
            long rangeLength = range.upperEndpoint() - range.lowerEndpoint();
            for (long totalDigestedZeros = 0L; totalDigestedZeros < rangeLength; totalDigestedZeros += _zerosBuffer.limit()) {
                assert totalDigestedZeros >= 0L;
                _zerosBuffer.clear();
                long limit = Math.min(_zerosBuffer.capacity(), rangeLength - totalDigestedZeros);
                _zerosBuffer.limit((int)limit);
                updateChecksum(_zerosBuffer, range.lowerEndpoint() + totalDigestedZeros, _zerosBuffer.limit());
            }
        }
    }

    private int writeWithChecksumUpdate(ByteBuffer src) throws IOException
    {
        int writtenBytes;
        ByteBuffer readOnly = src.asReadOnlyBuffer();
        long updatePosition = position();
        writtenBytes = _channel.write(src);
        updateChecksum(readOnly, updatePosition, writtenBytes);

        return writtenBytes;
    }

    /**
     * @param buffer buffer containing the data
     * @param position position of the data in the target file
     * @param bytes number of bytes to use from the input data
     * @throws IOException
     */
    @VisibleForTesting
    synchronized void updateChecksum(ByteBuffer buffer, long position, long bytes) throws IOException {
        if (bytes == 0)
            return;

        if (bytes < buffer.remaining()) {
            buffer.limit(buffer.position() + (int)bytes);
        }

        Range<Long> writeRange = Range.closed(position, position + buffer.remaining() - 1).canonical(DiscreteDomain.longs());

        RangeSet<Long> overlappingRanges = _dataRangeSet.subRangeSet(writeRange);
        if (!overlappingRanges.isEmpty()) {
            _isChecksumViable = false;
            _log.info("On-transfer checksum aborted due to overlapping writes from client.");
            return;
        }

        _dataRangeSet.add(writeRange);
        if (!_fileStartRange.isConnected(writeRange)) {
            return;
        }

        long digestStart = _fileStartRange.upperEndpoint();
        _fileStartRange = _dataRangeSet.rangeContaining(0L);
        long digestEnd = _fileStartRange.upperEndpoint();

        digestStart += buffer.remaining();
        _digest.update(buffer);
        long bytesToRead = digestEnd - digestStart;
        long lastBytesRead;
        for (long totalBytesRead = 0; totalBytesRead < bytesToRead; totalBytesRead += lastBytesRead) {
            _readBackBuffer.clear();
            long limit = Math.min(_readBackBuffer.capacity(), bytesToRead - totalBytesRead);
            _readBackBuffer.limit((int)limit);
            lastBytesRead = _channel.read(_readBackBuffer, digestStart + totalBytesRead);
            if (lastBytesRead < 0) {
                throw new IOException("Checksum: Unexpectedly hit end-of-stream while reading data back from channel.");
            }
            _readBackBuffer.flip();
            _digest.update(_readBackBuffer);
        }
    }
}
