package org.dcache.pool.statistics;

import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import org.dcache.pool.repository.ForwardingRepositoryChannel;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.LineIndentingPrintWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class decorates any RepositoryChannel and updates statistics for monitored per-request
 * quantities: requested bytes, transferred bytes, various durations, instantaneous bandwidth, and
 * concurrency (number of concurrent in-flight requests).  It also keeps track of how much time is
 * spent with at least one IO request (i.e., when blocking) and the different phases of a transfer:
 * pre-transfer, transfer, post-transfer.
 * <p>
 * Hint: It might be interesting for further developments to have a closer look at return values
 * from read and write methods, when they equal 0
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

    private final Stopwatch readIdle = Stopwatch.createUnstarted();
    private final Stopwatch readActive = Stopwatch.createUnstarted();
    private final Stopwatch writeIdle = Stopwatch.createUnstarted();
    private final Stopwatch writeActive = Stopwatch.createUnstarted();

    private int concurrentReads;
    private int concurrentWrites;

    private final Instant whenOpened = Instant.now(); // assuming created when channel is opened.
    private Instant firstRead;
    private Instant latestRead;
    private Instant firstWrite;
    private Instant latestWrite;
    private Instant whenClosed;

    public IoStatisticsChannel(RepositoryChannel channel) {
        this.channel = channel;
    }

    @Override
    protected RepositoryChannel delegate() {
        return channel;
    }

    /**
     * Returns an immutable description of activity.
     */
    public IoStatistics getStatistics() {
        Duration readIdleNow;
        Duration readActiveNow;
        Duration writeIdleNow;
        Duration writeActiveNow;
        Duration preReadActivityWait;
        Duration preWriteActivityWait;
        Duration postReadActivityWait;
        Duration postWriteActivityWait;

        synchronized (this) {
            Instant now = Instant.now();

            Instant firstReadOrNow = firstRead == null ? now : firstRead;
            Instant firstWriteOrNow = firstWrite == null ? now : firstWrite;
            Instant lastestReadOrNow = latestRead == null ? now : latestRead;
            Instant lastestWriteOrNow = latestWrite == null ? now : latestWrite;
            Instant whenClosedOrNow = whenClosed == null ? now : whenClosed;

            preReadActivityWait = Duration.between(whenOpened, firstReadOrNow);
            preWriteActivityWait = Duration.between(whenOpened, firstWriteOrNow);
            postReadActivityWait = Duration.between(lastestReadOrNow, whenClosedOrNow);
            postWriteActivityWait = Duration.between(lastestWriteOrNow, whenClosedOrNow);

            readIdleNow = Duration.ofNanos(readIdle.elapsed(TimeUnit.NANOSECONDS))
                  .minus(postReadActivityWait);
            readActiveNow = Duration.ofNanos(readActive.elapsed(TimeUnit.NANOSECONDS));
            writeIdleNow = Duration.ofNanos(writeIdle.elapsed(TimeUnit.NANOSECONDS))
                  .minus(postWriteActivityWait);
            writeActiveNow = Duration.ofNanos(writeActive.elapsed(TimeUnit.NANOSECONDS));
        }

        return new IoStatistics(
              new DirectedIoStatistics(preReadActivityWait, readIdleNow,
                    readActiveNow, firstRead, latestRead,
                    postReadActivityWait, reads),
              new DirectedIoStatistics(preWriteActivityWait, writeIdleNow,
                    writeActiveNow, firstWrite, latestWrite,
                    postWriteActivityWait, writes));
    }

    @GuardedBy("this")
    private boolean isClosed() {
        return whenClosed != null;
    }

    private synchronized int writeStarted() {
        Instant now = Instant.now();
        if (firstWrite == null) {
            firstWrite = now;
        }
        latestWrite = now;

        if (concurrentWrites++ == 0) {
            if (writeIdle.isRunning()) {
                writeIdle.stop();
            }

            if (!isClosed()) {
                writeActive.start();
            }
        }
        return concurrentWrites;
    }

    private synchronized void writeCompleted() {
        if (--concurrentWrites == 0) {
            if (writeActive.isRunning()) {
                writeActive.stop();
            }

            if (!isClosed()) {
                writeIdle.start();
            }
        }
    }

    private synchronized int readStarted() {
        Instant now = Instant.now();
        if (firstRead == null) {
            firstRead = now;
        }
        latestRead = now;

        if (concurrentReads++ == 0) {
            if (readIdle.isRunning()) {
                readIdle.stop();
            }

            if (!isClosed()) {
                readActive.start();
            }
        }
        return concurrentReads;
    }

    private synchronized void readCompleted() {
        if (--concurrentReads == 0) {
            if (readActive.isRunning()) {
                readActive.stop();
            }

            if (!isClosed()) {
                readIdle.start();
            }
        }
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
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
    public int read(ByteBuffer buffer, long position) throws IOException {
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
          throws IOException {
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
          throws IOException {
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
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
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
    public long write(ByteBuffer[] srcs) throws IOException {
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
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
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
    public long read(ByteBuffer[] dsts) throws IOException {
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
    public int read(ByteBuffer dst) throws IOException {
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
    public int write(ByteBuffer src) throws IOException {
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
    public void close() throws IOException {
        synchronized (this) {
            if (!isClosed()) {
                whenClosed = Instant.now();

                if (concurrentReads == 0) {
                    if (readIdle.isRunning()) {
                        readIdle.stop();
                    }
                } else {
                    LOGGER.warn("close called with in-flight read request");
                    // allow in-flight read request(s) to stop readActive stopwatch.
                }

                if (concurrentWrites == 0) {
                    if (writeIdle.isRunning()) {
                        writeIdle.stop();
                    }
                } else {
                    LOGGER.warn("close called with in-flight write request");
                    // allow in-flight write request(s) to stop writeActive stopwatch.
                }
            }
        }

        channel.close();
    }

    public void getInfo(PrintWriter pw) {
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
