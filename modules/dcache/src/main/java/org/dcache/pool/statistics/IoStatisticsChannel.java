package org.dcache.pool.statistics;

import org.dcache.pool.repository.RepositoryChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * This class decorates any RepositoryChannel and adds the function of collecting data for the IoStatistics
 */
public class IoStatisticsChannel implements RepositoryChannel {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(IoStatisticsChannel.class);

    /**
     * Inner channel to which all operations are delegated.
     */
    private RepositoryChannel _channel;

    private final IoStatistics _statistics = new IoStatistics();

    public IoStatisticsChannel(RepositoryChannel channel){ _channel = channel; }

    /**
     * Returns the object most central of this decorator
     * @return object with collected and evaluated statistics data
     */
    public IoStatistics getStatistics() {
        return _statistics;
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
        long startTime = System.nanoTime();
        int writtenBytes = _channel.write(buffer, position); // might be 0 if nothing has been written
        long duration = System.nanoTime() - startTime;
        _statistics.updateWrite(writtenBytes, duration);
        return writtenBytes;
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        long startTime = System.nanoTime();
        int readBytes = _channel.read(buffer, position); // -1 => position greater than file; 0 => if end of file
        long duration = System.nanoTime() - startTime;
        _statistics.updateRead(readBytes, duration);
        return readBytes;
    }

    @Override
    public void sync() throws SyncFailedException, IOException {
        _channel.sync();
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        long startTime = System.nanoTime();
        long readBytes =  _channel.transferTo(position, count, target);
        long duration = System.nanoTime() - startTime;
        _statistics.updateRead(readBytes, duration);
        return readBytes;
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        long startTime = System.nanoTime();
        long writtenBytes =  _channel.transferFrom(src, position, count);
        long duration = System.nanoTime() - startTime;
        _statistics.updateWrite(writtenBytes, duration);
        return writtenBytes;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        long startTime = System.nanoTime();
        long writtenBytes = _channel.write(srcs, offset, length);
        long duration = System.nanoTime() - startTime;
        _statistics.updateWrite(writtenBytes, duration);
        return writtenBytes;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        long startTime = System.nanoTime();
        long writtenBytes = _channel.write(srcs);
        long duration = System.nanoTime() - startTime;
        _statistics.updateWrite(writtenBytes, duration);
        return writtenBytes;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        long startTime = System.nanoTime();
        long readBytes = _channel.read(dsts, offset, length);
        long duration = System.nanoTime() - startTime;
        _statistics.updateRead(readBytes, duration);
        return readBytes;
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        long startTime = System.nanoTime();
        long readBytes = _channel.read(dsts);
        long duration = System.nanoTime() - startTime;
        _statistics.updateRead(readBytes, duration);
        return readBytes;
    }

    @Override
    public long position() throws IOException {
        return _channel.position();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        return _channel.position(newPosition);
    }

    @Override
    public long size() throws IOException {
        return _channel.size();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        return _channel.truncate(size);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        long startTime = System.nanoTime();
        int readBytes = _channel.read(dst);
        long duration = System.nanoTime() - startTime;
        _statistics.updateRead(readBytes, duration);
        return readBytes;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        long startTime = System.nanoTime();
        int writtenBytes = _channel.write(src);
        long duration = System.nanoTime() - startTime;
        _statistics.updateWrite(writtenBytes, duration);
        return writtenBytes;
    }

    @Override
    public boolean isOpen() {
        return _channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        _channel.close();
    }
}
