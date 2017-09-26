package org.dcache.pool.statistics;

import org.dcache.pool.repository.ForwardingRepositoryChannel;
import org.dcache.pool.repository.RepositoryChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * This class decorates any RepositoryChannel and adds the function of collecting data for the IoStatistics
 * Hint: It might be interesting for further developments to have a closer look at return values from read and write methods, when they equal 0
 */
public class IoStatisticsChannel extends ForwardingRepositoryChannel {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(IoStatisticsChannel.class);

    /**
     * Inner channel to which all operations are delegated.
     */
    private final RepositoryChannel channel;

    private final IoStatistics statistics = new IoStatistics();

    public IoStatisticsChannel(RepositoryChannel channel){
        this.channel = channel;
    }

    @Override
    protected RepositoryChannel delegate() {
        return channel;
    }

    /**
     * Returns the object most central of this decorator
     * @return object with collected and evaluated statistics data
     */
    public IoStatistics getStatistics() {
        return statistics;
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
        long requestedWriteBytes = buffer.limit() - buffer.position();
        long startTime = System.nanoTime();
        int writtenBytes = channel.write(buffer, position);
        long duration = System.nanoTime() - startTime;
        statistics.updateWrite(writtenBytes, duration, requestedWriteBytes);
        return writtenBytes;
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        long requestedReadBytes = buffer.limit() - buffer.position();
        long startTime = System.nanoTime();
        int readBytes = channel.read(buffer, position);
        long duration = System.nanoTime() - startTime;
        statistics.updateRead(readBytes, duration, requestedReadBytes);
        return readBytes;
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        long startTime = System.nanoTime();
        long readBytes =  channel.transferTo(position, count, target);
        long duration = System.nanoTime() - startTime;
        statistics.updateRead(readBytes, duration, count);
        return readBytes;
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        long startTime = System.nanoTime();
        long writtenBytes =  channel.transferFrom(src, position, count);
        long duration = System.nanoTime() - startTime;
        statistics.updateWrite(writtenBytes, duration, count);
        return writtenBytes;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        long requestedWriteBytes = 0;
        for (int i = offset; i < (offset + length); i++){
            requestedWriteBytes += srcs[i].limit() - srcs[i].position();
        }
        long startTime = System.nanoTime();
        long writtenBytes = channel.write(srcs, offset, length);
        long duration = System.nanoTime() - startTime;
        statistics.updateWrite(writtenBytes, duration, requestedWriteBytes);
        return writtenBytes;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        long requestedWriteBytes = 0;
        for (int i = 0; i < srcs.length; i++){
            requestedWriteBytes += srcs[i].limit() - srcs[i].position();
        }
        long startTime = System.nanoTime();
        long writtenBytes = channel.write(srcs);
        long duration = System.nanoTime() - startTime;
        statistics.updateWrite(writtenBytes, duration, requestedWriteBytes);
        return writtenBytes;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        long requestedReadBytes = 0;
        for (int i = offset; i < (offset + length); i++){
            requestedReadBytes += dsts[i].limit() - dsts[i].position();
        }
        long startTime = System.nanoTime();
        long readBytes = channel.read(dsts, offset, length);
        long duration = System.nanoTime() - startTime;
        statistics.updateRead(readBytes, duration, requestedReadBytes);
        return readBytes;
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        long requestedReadBytes = 0;
        for (int i = 0; i < dsts.length; i++){
            requestedReadBytes += dsts[i].limit() - dsts[i].position();
        }
        long startTime = System.nanoTime();
        long readBytes = channel.read(dsts);
        long duration = System.nanoTime() - startTime;
        statistics.updateRead(readBytes, duration, requestedReadBytes);
        return readBytes;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        long requestedReadBytes = dst.limit() - dst.position();
        long startTime = System.nanoTime();
        int readBytes = channel.read(dst);
        long duration = System.nanoTime() - startTime;
        statistics.updateRead(readBytes, duration, requestedReadBytes);
        return readBytes;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        long requestedWriteBytes = src.limit() - src.position();
        long startTime = System.nanoTime();
        int writtenBytes = channel.write(src);
        long duration = System.nanoTime() - startTime;
        statistics.updateWrite(writtenBytes, duration, requestedWriteBytes);
        return writtenBytes;
    }
}
