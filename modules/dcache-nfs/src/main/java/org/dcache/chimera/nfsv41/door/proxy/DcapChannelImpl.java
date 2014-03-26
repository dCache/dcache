package org.dcache.chimera.nfsv41.door.proxy;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

/**
 * DCAP based implementation of {@link ProxyIoAdapter}.
 */
public class DcapChannelImpl implements ProxyIoAdapter {

    /**
     * DCAP binary commands
     */
    private static final int CLOSE = 4;
    private static final int SEEK_AND_READ = 11;
    private static final int SEEK_AND_WRITE = 12;

    /**
     * Protocol constants
     */
    private static final int DATA = 8;
    private static final int EOD = -1;

    /**
     * DCAP SEEK whence
     */
    private static final int SEEK_SET = 0;

    private final SocketChannel _channel;
    private final long _size;

    public DcapChannelImpl(InetSocketAddress addr, int session, byte[] challange, long size) throws IOException {
        _channel = SocketChannel.open(addr);
        _channel.configureBlocking(true);
        ByteBuffer buf = ByteBuffer.allocate(8 + challange.length);
        buf.order(ByteOrder.BIG_ENDIAN);
        buf.putInt(session);
        buf.putInt(challange.length).put(challange);
        buf.flip();
        writeFully(_channel, buf);
        _size = size;
    }

    @Override
    public synchronized int read(ByteBuffer dst, long position) throws IOException {

        ByteBuffer command = new DcapCommandBuilder()
                .withSeekAndRead(position, dst.remaining())
                .build();

        writeFully(_channel, command);
        getAck();
        return getData(dst);
    }

    @Override
    public synchronized int write(ByteBuffer src, long position) throws IOException {
        ByteBuffer command = new DcapCommandBuilder()
                .withSeekAndWrite(position, src.remaining())
                .build();

        writeFully(_channel, command);
        getAck();
        return sendData(src);
    }

    @Override
    public long size() {
        return _size;
    }

    @Override
    public synchronized void close() throws IOException {
        ByteBuffer command = new DcapCommandBuilder()
                .withClose()
                .build();

        writeFully(_channel, command);
        getAck();
        _channel.close();
    }

    private int getData(ByteBuffer buf) throws IOException {
        ByteBuffer dataBlock = ByteBuffer.allocate(128);
        dataBlock.order(ByteOrder.BIG_ENDIAN);

        dataBlock.limit(8);
        readFully(_channel, dataBlock);
        int total = 0;
        while (true) {
            dataBlock.clear();
            dataBlock.limit(4);
            readFully(_channel, dataBlock);
            dataBlock.flip();
            int n = dataBlock.getInt();
            if (n < 0) {
                getAck();
                break;
            }

            ByteBuffer chunk = buf.slice();
            chunk.limit(n);
            readFully(_channel, chunk);
            buf.position(buf.position() + n);
            total += n;
        }
        return total;
    }

    private int sendData(ByteBuffer b) throws IOException {
        int nbytes = b.remaining();
        ByteBuffer dataBlock = ByteBuffer.allocate(12);
        dataBlock.order(ByteOrder.BIG_ENDIAN);
        dataBlock.putInt(4);
        dataBlock.putInt(DATA);
        dataBlock.putInt(nbytes);
        dataBlock.flip();

        writeFully(_channel, dataBlock);
        writeFully(_channel, b);
        dataBlock.clear();
        dataBlock.putInt(EOD);
        dataBlock.flip();
        writeFully(_channel, dataBlock);
        getAck();
        return nbytes;
    }

    private void getAck() throws IOException {
        ByteBuffer ackBuffer = ByteBuffer.allocate(256);
        ackBuffer.order(ByteOrder.BIG_ENDIAN);
        ackBuffer.limit(4);
        readFully(_channel, ackBuffer);
        ackBuffer.flip();
        int len = ackBuffer.getInt();
        ackBuffer.clear().limit(len);
        readFully(_channel, ackBuffer);
        // FIXME: error handling
    }


    private static void writeFully(SocketChannel channel, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    private static void readFully(SocketChannel channel, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            if (channel.read(buf) < 0) {
                throw new EOFException("EOF on input socket (fillBuffer)");
            }
        }
    }

    private static class DcapCommandBuilder {

        private final ByteBuffer _command;

        DcapCommandBuilder() {
            _command = ByteBuffer.allocate(8192);
            _command.order(ByteOrder.BIG_ENDIAN);
            _command.position(Integer.SIZE / 8);
        }

        DcapCommandBuilder withSeekAndRead(long offset, long len) {
            _command.putInt(SEEK_AND_READ);
            _command.putLong(offset);
            _command.putInt(SEEK_SET);
            _command.putLong(len);
            return this;
        }

        DcapCommandBuilder withSeekAndWrite(long offset, int len) {
            _command.putInt(SEEK_AND_WRITE);
            _command.putLong(offset);
            _command.putInt(SEEK_SET);
            return this;
        }

        DcapCommandBuilder withClose() {
            _command.putInt(CLOSE);
            return this;
        }

        DcapCommandBuilder withByteCount(long count) {
            _command.putLong(count);
            return this;
        }

        ByteBuffer build() {
            _command.putInt(0, _command.position() - 4);
            _command.flip();
            return _command;
        }
    }
}
