package org.dcache.pool.statistics;

import org.dcache.pool.movers.ChecksumChannel;
import org.dcache.pool.repository.RepositoryChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

public class IoStatsChannel implements StatsChannel{

    private static final Logger LOGGER =
            LoggerFactory.getLogger(IoStatsChannel.class);

    RepositoryChannel _channel;

    private final Statistics statistics = new Statistics();

    public IoStatsChannel(RepositoryChannel channel){
        _channel = channel;
    }

    @Override
    public Statistics getStats() {
        return statistics;
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
        long startTime = System.currentTimeMillis();
        int writtenBytes = _channel.write(buffer, position); // might be 0 if nothing has been written
        long duration = startTime - System.currentTimeMillis();
        return writtenBytes;
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        long startTime = System.currentTimeMillis();
        int readBytes = _channel.read(buffer, position); // -1 => position greater than file; 0 => if end of file
        long duration = startTime - System.currentTimeMillis();
        return readBytes;
    }

    @Override
    public void sync() throws SyncFailedException, IOException {

    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return 0;
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        return 0;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return 0;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        long startTime = System.currentTimeMillis();
        long writtenBytes = _channel.write(srcs);
        long duration = startTime - System.currentTimeMillis();
        return writtenBytes;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        long startTime = System.currentTimeMillis();
        long readBytes = _channel.read(dsts, offset, length);
        long duration = startTime - System.currentTimeMillis();
        return readBytes;
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        long startTime = System.currentTimeMillis();
        long readBytes = _channel.read(dsts);
        long duration = startTime - System.currentTimeMillis();
        return readBytes;
    }

    @Override
    public long position() throws IOException {
        return 0;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        return null;
    }

    @Override
    public long size() throws IOException {
        return 0;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        return null;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        long startTime = System.currentTimeMillis();
        int readBytes = _channel.read(dst);
        long duration = startTime - System.currentTimeMillis();
        return readBytes;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        long supposedBytes = src.limit() - src.position();
        long startTime = System.currentTimeMillis();
        int writtenBytes = _channel.write(src);
        long duration = startTime - System.currentTimeMillis();
        return writtenBytes;
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void close() throws IOException {

    }
}
