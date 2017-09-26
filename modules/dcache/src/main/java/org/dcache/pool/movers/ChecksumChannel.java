package org.dcache.pool.movers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.dcache.pool.repository.ForwardingRepositoryChannel;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static org.dcache.util.ByteUnit.KiB;

/**
 * A wrapper for RepositoryChannel that computes a digest
 * on the fly during write as long as all writes are
 * sequential.
 */
public class ChecksumChannel extends ForwardingRepositoryChannel
{
    private static final Logger _log =
            LoggerFactory.getLogger(ChecksumChannel.class);

    /**
     * Inner channel to which all operations are delegated.
     */
    @VisibleForTesting
    RepositoryChannel _channel;

    /**
     * Digest used for computing the checksum during write.
     */
    private final List<MessageDigest> _digests;

    /**
     * Cached checksum after getChecksums is called the first time.
     */
    private Set<Checksum> _finalChecksums;

    /**
     * RangeSet to keep track of written bytes
     */
    private final RangeSet<Long> _dataRangeSet = TreeRangeSet.create();

    /**
     * The offset where the checksum was calculated.
     */
    @GuardedBy("_digests")
    private long _nextChecksumOffset = 0L;

    /**
     * Flag to indicate whether it is still possible to calculated a checksum
     */
    private volatile boolean _isChecksumViable = true;

    /**
     * Flag to indicate whether we still allow writing to the channel.
     * This flag is set to false after getChecksums has been called.
     */
    @GuardedBy("_ioStateLock")
    private boolean _isWritable = true;

    /**
     * Lock to protect _isWritable field.
     */
    private final ReentrantReadWriteLock _ioStateLock = new ReentrantReadWriteLock();
    private final Lock _ioStateReadLock = _ioStateLock.readLock();
    private final Lock _ioStateWriteLock = _ioStateLock.writeLock();

    /**
     * Buffer to be used for reading data back from the inner channel for
     * checksum calculations.
     */
    @VisibleForTesting
    ByteBuffer _readBackBuffer = ByteBuffer.allocate(KiB.toBytes(256));

    /*
     * Static buffer with zeros shared with in all instances of ChecksumChannel.
     */
    private static final ByteBuffer ZERO_BUFFER = ByteBuffer
            .allocate(KiB.toBytes(256))
            .asReadOnlyBuffer();

    /**
     * Buffer to be used for feeding the checksum digester with 0s to fill up
     * gaps in ranges.
     */
    @VisibleForTesting
    ByteBuffer _zerosBuffer = ZERO_BUFFER.duplicate();

    public ChecksumChannel(RepositoryChannel inner, Set<ChecksumType> types)
    {
        _channel = inner;
        _digests = types.stream()
                .map(t -> t.createMessageDigest())
                .collect(Collectors.toList());
    }

    @Override
    protected RepositoryChannel delegate() {
        return _channel;
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
        _ioStateReadLock.lock();
        try {
            checkState(_isWritable, "ChecksumChannel must not be written to after getChecksums");

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
            _ioStateReadLock.unlock();
        }
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
        _ioStateReadLock.lock();
        try {

            checkState(_isWritable, "ChecksumChannel must not be written to after getChecksums");

            int bytes;
            if (_isChecksumViable) {
                bytes = writeWithChecksumUpdate(src);
            } else {
                bytes = _channel.write(src);
            }
            return bytes;
        } finally {
            _ioStateReadLock.unlock();
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException {
        _ioStateReadLock.lock();
        try {

            checkState(_isWritable, "ChecksumChannel must not be written to after getChecksums");

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
            _ioStateReadLock.unlock();
        }
    }

    @Override
    public synchronized long write(ByteBuffer[] srcs) throws IOException
    {
        return write(srcs, 0, srcs.length);
    }

    /**
     * @return final checksum of this channel
     */
    public Set<Checksum> getChecksums()
    {
        if (!_isChecksumViable) {
            return Collections.emptySet();
        }

        if (_finalChecksums == null) {
            _finalChecksums = finalizeChecksums();
        }
        return _finalChecksums;
    }

    /**
     * Returns the computed digest or null if overlapping writes have been detected.
     *
     * @return Checksum
     */
    private Set<Checksum> finalizeChecksums() {

        _ioStateWriteLock.lock();
        try {
            _isWritable = false;
        } finally {
            _ioStateWriteLock.unlock();
        }

        // we need to synchronize on rangeSet and digest get exclusive access
        synchronized (_dataRangeSet) {
            synchronized (_digests) {
                try {

                    if (_dataRangeSet.asRanges().size() != 1 || _nextChecksumOffset == 0) {
                        feedZerosToDigesterForRangeGaps();
                    }

                    return _digests.stream()
                            .map(Checksum::new)
                            .collect(Collectors.toSet());
                } catch (IOException e) {
                    _log.info("Unable to generate checksum of sparse file: {}", e.toString());
                    return Collections.emptySet();
                }
            }
        }
    }

    private void feedZerosToDigesterForRangeGaps() throws IOException {
        ArrayList<Range<Long>> complement = newArrayList(_dataRangeSet.complement().subRangeSet(Range.closed(0L, size())).asRanges());
        complement.sort((r1, r2) -> r1.lowerEndpoint().compareTo(r2.lowerEndpoint()));

        for (Range<Long> range : complement) {

            long bytesToWrite = range.upperEndpoint() - range.lowerEndpoint();
            long chunkOffset = range.lowerEndpoint();

            while (bytesToWrite > 0) {
                _zerosBuffer.clear();
                long chunkSize = Math.min(_zerosBuffer.capacity(), bytesToWrite);
                _zerosBuffer.limit((int)chunkSize);

                updateChecksum(_zerosBuffer, chunkOffset, _zerosBuffer.limit());

                chunkOffset += chunkSize;
                bytesToWrite -= chunkSize;
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
    void updateChecksum(ByteBuffer buffer, long position, int bytes) throws IOException {

        if (bytes == 0) {
            return;
        }

        if (bytes < buffer.remaining()) {
            buffer.limit(buffer.position() + bytes);
        }

        Range<Long> writeRange = Range.closed(position, position + buffer.remaining() - 1).canonical(DiscreteDomain.longs());
        Range<Long> fileStartRange;

        synchronized (_dataRangeSet) {

            RangeSet<Long> overlappingRanges = _dataRangeSet.subRangeSet(writeRange);
            if (!overlappingRanges.isEmpty()) {
                _isChecksumViable = false;
                _log.info("On-transfer checksum aborted due to overlapping writes from client.");
                return;
            }

            fileStartRange = _dataRangeSet.rangeContaining(0L);
            boolean canCalculateChecksum = position == 0 || (fileStartRange != null && fileStartRange.upperEndpoint() == position);

            _dataRangeSet.add(writeRange);
            if (!canCalculateChecksum) {
                return;
            }

            // get it again as we may have merged two segments
            fileStartRange = _dataRangeSet.rangeContaining(0L);
        }

        synchronized (_digests) {
            /*
             * we are one of the threads which got the merge into continues block.
             * Nevertheless, there may be a different thread which needs to update
             * ahead of us. Wait for our turn.
             */
            while(_nextChecksumOffset != position) {
                try {
                    _digests.wait();
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }

            long bytesToRead = fileStartRange.upperEndpoint() - position;

            // update current buffer and then keep procesing following blocks, if any
            bytesToRead -= buffer.remaining();
            // update offset prior digest calculation as digests#update will update position in the buffer
            _nextChecksumOffset += buffer.remaining();

            _digests.forEach(d -> d.update(buffer.duplicate()));

            long expectedOffsetAfterRead = _nextChecksumOffset + bytesToRead;
            try
            {
                while (bytesToRead > 0) {
                    _readBackBuffer.clear();
                    long limit = Math.min(_readBackBuffer.capacity(), bytesToRead);
                    _readBackBuffer.limit((int) limit);
                    int lastBytesRead = _channel.read(_readBackBuffer, _nextChecksumOffset);

                    if (lastBytesRead < 0) {
                        throw new IOException("Checksum: Unexpectedly hit end-of-stream while reading data back from channel.");
                    }

                    _readBackBuffer.flip();

                    _digests.forEach(d -> d.update(_readBackBuffer.duplicate()));

                    bytesToRead -= lastBytesRead;
                    _nextChecksumOffset += lastBytesRead;
                }

            } catch (IOException | RuntimeException e) {
                _isChecksumViable = false;
                throw e;
            } finally {
                _nextChecksumOffset = expectedOffsetAfterRead;
                _digests.notifyAll();
            }
        }
    }
}
