package org.dcache.pool.statistics;

import com.google.common.base.Stopwatch;

import org.dcache.pool.repository.ForwardingRepositoryChannel;
import org.dcache.pool.repository.RepositoryChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.dcache.util.LineIndentingPrintWriter;

/**
 * This class decorates any RepositoryChannel and updates statistics for
 * monitored per-request quantities: requested bytes, transferred bytes,
 * duration, instantaneous bandwidth, and concurrency (number of concurrent
 * in-flight requests).  It also keeps track of how much time is spent with at
 * least one IO request.
 * <p>
 * Hint: It might be interesting for further developments to have a closer look
 * at return values from read and write methods, when they equal 0
 */
public class IoStatisticsChannel extends ForwardingRepositoryChannel {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(IoStatisticsChannel.class);

    /**
     * Inner channel to which all operations are delegated.
     */
    private final RepositoryChannel channel;


    private final LiveStatistics reads = new LiveStatistics();
    private final LiveStatistics writes = new LiveStatistics();

    private final Stopwatch readIdle = Stopwatch.createStarted();
    private final Stopwatch readActive = Stopwatch.createUnstarted();
    private final Stopwatch writeIdle = Stopwatch.createStarted();
    private final Stopwatch writeActive = Stopwatch.createUnstarted();

    private int concurrentReads;
    private int concurrentWrites;
    private boolean isClosed;
    private Instant firstRead;
    private Instant latestRead;
    private Instant firstWrite;
    private Instant latestWrite;

    public IoStatisticsChannel(RepositoryChannel channel)
    {
        this.channel = channel;
    }

    @Override
    protected RepositoryChannel delegate()
    {
        return channel;
    }

    /**
     * Returns an immutable description of activity.
     */
    public IoStatistics getStatistics()
    {
        Duration readIdleNow;
        Duration readActiveNow;
        Duration writeIdleNow;
        Duration writeActiveNow;

        synchronized (this) {
            readIdleNow = Duration.ofNanos(readIdle.elapsed(TimeUnit.NANOSECONDS));
            readActiveNow = Duration.ofNanos(readActive.elapsed(TimeUnit.NANOSECONDS));
            writeIdleNow = Duration.ofNanos(writeIdle.elapsed(TimeUnit.NANOSECONDS));
            writeActiveNow = Duration.ofNanos(writeActive.elapsed(TimeUnit.NANOSECONDS));
        }

        return new IoStatistics(
                new DirectedIoStatistics(readIdleNow, readActiveNow,
                        firstRead, latestRead, reads),
                new DirectedIoStatistics(writeIdleNow, writeActiveNow,
                        firstWrite, latestWrite, writes));
    }

    private synchronized int writeStarted()
    {
        Instant now = Instant.now();
        if (firstWrite == null) {
            firstWrite = now;
        }
        latestWrite = now;
        if (!isClosed && concurrentWrites == 0) {
            writeIdle.stop();
            writeActive.start();
        }
        return ++concurrentWrites;
    }

    private synchronized void writeCompleted()
    {
        if (--concurrentWrites == 0) {
            writeActive.stop();
            if (!isClosed) {
                writeIdle.start();
            }
        }
    }

    private synchronized int readStarted()
    {
        Instant now = Instant.now();
        if (firstRead == null) {
            firstRead = now;
        }
        latestRead = now;
        if (!isClosed && concurrentReads == 0) {
            readIdle.stop();
            readActive.start();
        }
        return ++concurrentReads;
    }

    private synchronized void readCompleted()
    {
        if (--concurrentReads == 0) {
            readActive.stop();
            if (!isClosed) {
                readIdle.start();
            }
        }
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException
    {
        int concurrency = writeStarted();
        try {
            long requested = buffer.limit() - buffer.position();
            long startTime = System.nanoTime();
            int transferred = channel.write(buffer, position);
            writes.accept(concurrency, requested, transferred, startTime);
            return transferred;
        } finally {
            writeCompleted();
        }
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException
    {
        int concurrency = readStarted();
        try {
            long requested = buffer.limit() - buffer.position();
            long startTime = System.nanoTime();
            int transferred = channel.read(buffer, position);
            reads.accept(concurrency, requested, transferred > 0 ? transferred : 0,
                    startTime);
            return transferred;
        } finally {
            readCompleted();
        }
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target)
            throws IOException
    {
        int concurrency = readStarted();
        try {
            long startTime = System.nanoTime();
            long transferred = channel.transferTo(position, count, target);
            reads.accept(concurrency, count, transferred, startTime);
            return transferred;
        } finally {
            readCompleted();
        }
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count)
            throws IOException
    {
        int concurrency = writeStarted();
        try {
            long startTime = System.nanoTime();
            long transferred = channel.transferFrom(src, position, count);
            writes.accept(concurrency, count, transferred, startTime);
            return transferred;
        } finally {
            writeCompleted();
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
    {
        int concurrency = writeStarted();
        try {
            long requested = Arrays.stream(srcs).skip(offset).limit(length)
                    .mapToLong(b -> b.limit() - b.position()).sum();
            long startTime = System.nanoTime();
            long transferred = channel.write(srcs, offset, length);
            writes.accept(concurrency, requested, transferred, startTime);
            return transferred;
        } finally {
            writeCompleted();
        }
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException
    {
        int concurrency = writeStarted();
        try {
            long requested = Arrays.stream(srcs)
                    .mapToLong(b -> b.limit() - b.position())
                    .sum();
            long startTime = System.nanoTime();
            long transferred = channel.write(srcs);
            writes.accept(concurrency, requested, transferred, startTime);
            return transferred;
        } finally {
            writeCompleted();
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException
    {
        int concurrency = readStarted();
        try {
            long requested = Arrays.stream(dsts).skip(offset).limit(length)
                    .mapToLong(b -> b.limit() - b.position()).sum();
            long startTime = System.nanoTime();
            long transferred = channel.read(dsts, offset, length);
            reads.accept(concurrency, requested, transferred > 0 ? transferred : 0,
                    startTime);
            return transferred;
        } finally {
            readCompleted();
        }
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException
    {
        int concurrency = readStarted();
        try {
            long requested = Arrays.stream(dsts)
                    .mapToLong(b -> b.limit() - b.position())
                    .sum();
            long startTime = System.nanoTime();
            long transferred = channel.read(dsts);
            reads.accept(concurrency, requested, transferred > 0 ? transferred : 0,
                    startTime);
            return transferred;
        } finally {
            readCompleted();
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        int concurrency = readStarted();
        try {
            long requested = dst.limit() - dst.position();
            long startTime = System.nanoTime();
            int transferred = channel.read(dst);
            reads.accept(concurrency, requested, transferred > 0 ? transferred : 0,
                    startTime);
            return transferred;
        } finally {
            readCompleted();
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        int concurrency = writeStarted();
        try {
            long requested = src.limit() - src.position();
            long startTime = System.nanoTime();
            int transferred = channel.write(src);
            writes.accept(concurrency, requested, transferred, startTime);
            return transferred;
        } finally {
            writeCompleted();
        }
    }

    @Override
    public void close() throws IOException
    {
        synchronized (this) {
            if (!isClosed) {
                isClosed = true;

                if (concurrentReads == 0) {
                    readIdle.stop();
                } else {
                    LOGGER.debug("close called with in-flight read request");
                }

                if (concurrentWrites == 0) {
                    writeIdle.stop();
                } else {
                    LOGGER.debug("close called with in-flight write request");
                }
            }
        }

        channel.close();
    }

    public void getInfo(PrintWriter pw)
    {
        IoStatistics stats = getStatistics();

        if (stats.hasReads() && stats.hasWrites()) {
            pw.println("Disk IO statistics:");
            stats.getInfo(new LineIndentingPrintWriter(pw, "    "));
        } else if (stats.hasReads()) {
            pw.println("Disk IO read statistics:");
            stats.getInfo(new LineIndentingPrintWriter(pw, "    "));
        } else if (stats.hasWrites()) {
            pw.println("Disk IO write statistics:");
            stats.getInfo(new LineIndentingPrintWriter(pw, "    "));
        }
    }
}
