package org.dcache.ftp.data;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.dcache.pool.repository.RepositoryChannel;

import static org.dcache.util.Exceptions.messageOrClassName;
import static org.dcache.util.Strings.describeSize;
import static org.dcache.util.Strings.toThreeSigFig;

/**
 * Implementation of MODE E.
 *
 * Be aware that it is quite easy to introduce race conditions, so
 * please keep this in mind when making changes. In particular the EOD
 * and EOD count handling is a little tricky.
 */
public class ModeE extends Mode
{
    /**
     * Header length of a mode E block.
     */
    public static final int HEADER_LENGTH = 17;

    public static final int EOR_DESCRIPTOR                       = 128;
    public static final int EOF_DESCRIPTOR                       = 64;
    public static final int SUSPECTED_ERROR_DESCRIPTOR           = 32;
    public static final int RESTART_MARKER_DESCRIPTOR            = 16;
    public static final int EOD_DESCRIPTOR                       = 8;
    public static final int SENDER_CLOSES_THIS_STREAM_DESCRIPTOR = 4;

    public static final int KNOWN_DESCRIPTORS =
        EOF_DESCRIPTOR | EOD_DESCRIPTOR | SENDER_CLOSES_THIS_STREAM_DESCRIPTOR;

    /**
     * The chunk size used when sending files.
     *
     * Large blocks will reduce the overhead of sending. However, it
     * case of multible concurrent streams, large blocks will make
     * disk access less sequential on both the sending and receiving
     * side.
     */
    private final int _blockSize;

    /** Position in file when sending data. Used by the sender. */
    private long _currentPosition;

    /**
     * Number of bytes that have to be transferred. Used by the sender.
     */
    private long _currentCount;

    /**
     * EOD count received. Zero as long as no EOD count was received.
     */
    private long _eodc;

    /**
     * Whether the transfer has started.
     */
    private volatile boolean _transferStarted;

    /**
     * Number of active channels.
     */
    private final AtomicLong _activeDataChannels = new AtomicLong();

    /**
     * Number of channels that closed in error.
     */
    private final LongAdder _errorDataChannels = new LongAdder();

    private String _lastError;

    /**
     * Implementation of send in mode E. There will be an instance per
     * data channel. The sender repeatedly bites _blockSize bytes of
     * the file and transfers it as a single block. I.e.
     * _currentPosition is incremented by _blockSize bytes at a time.
     */
    private class Sender extends AbstractMultiplexerListener
    {
        protected static final int PREPARE_BLOCK = 0;
        protected static final int SEND_HEADER = 1;
        protected static final int SEND_DATA = 2;

        /** Socket used for data channel. */
        protected final SocketChannel _socket;

        /** State of the sender. */
        protected int           _state;

        /** Position in file, which we will send next. */
        protected long          _position;

        /** Bytes remaining from current block. */
        protected long          _count;

        /** True if this sender must send the EOF. */
        protected final boolean       _sendEOF;

        /** Buffer for sending the block header. */
        protected final ByteBuffer _header =
            ByteBuffer.allocate(HEADER_LENGTH);

        public Sender(SocketChannel socket) {
            _socket  = socket;
            _state   = PREPARE_BLOCK;
            _sendEOF = (_opened == 1); // First sender sends EOF
        }

        @Override
        public void register(Multiplexer multiplexer) throws IOException
        {
            multiplexer.register(this, SelectionKey.OP_WRITE, _socket);
        }

        @Override
        public void write(Multiplexer multiplexer, SelectionKey key)
            throws IOException, FTPException
        {
            try {
                doWrite(multiplexer, key);
            } catch (IOException | FTPException e) {
                _activeDataChannels.decrementAndGet();
                _errorDataChannels.increment();
                _lastError = messageOrClassName(e);
                throw e;
            }
        }

        private void doWrite(Multiplexer multiplexer, SelectionKey key)
                throws IOException, FTPException
        {
            switch (_state) {
            case PREPARE_BLOCK:
                /* Prepare new block. We 'bite' up to _blockSize bytes
                 * of the file and reserve it for this data channel.
                 */
                _position         = _currentPosition;
                _count            = Math.min(_currentCount, _blockSize);
                _currentPosition += _count;
                _currentCount    -= _count;

                /* Prepare header.
                 */
                _header.clear();
                if (_count > 0) {
                    // Regular block.
                    _header.put((byte)0);
                    _header.putLong(_count);         // Count
                    _header.putLong(_position);      // Position
                } else if (_sendEOF) {
                    /* This would fail if we are a passive
                     * sender. Luckily, senders are never passive.
                     */
                    if (!waitForConnectionCompletion(key)) {
                        return;
                    }
                    // Send EOD and EOD count. Since all connections
                    // have been established by now, we know that
                    // _opened is the actual number of connections
                    // that have been established.
                    _header.put((byte)(EOF_DESCRIPTOR | EOD_DESCRIPTOR | SENDER_CLOSES_THIS_STREAM_DESCRIPTOR));
                    _header.putLong(0);              // Unused
                    _header.putLong(_opened);        // EOD count
                } else {
                    // No more data. Send EOD.
                    _header.put((byte)(EOD_DESCRIPTOR | SENDER_CLOSES_THIS_STREAM_DESCRIPTOR));
                    _header.putLong(0);              // Count
                    _header.putLong(0);              // Position
                }
                _header.flip();
                _state = SEND_HEADER;

            case SEND_HEADER:
                /* Send header.
                 */
                _socket.write(_header);

                if (_header.position() < _header.limit()) {
                    break;
                }

                /* If at end of stream, close the channel.
                 *
                 * Notice that we allow close() to shut down the
                 * multiplexer if all connections have been closed
                 * (the third argument is true). This is valid because
                 * the first sender being created does not close the
                 * connection until it has sent EOF, and it does not
                 * sent EOF until all connections have been
                 * established.
                 */
                if (_count == 0) {
                    close(multiplexer, key, true);
                    _activeDataChannels.decrementAndGet();
                    break;
                }
                _state = SEND_DATA;

            case SEND_DATA:
                /* Send data.
                 */
                long nbytes = transferTo(_position, _count, _socket);
                _monitor.sentBlock(_position, nbytes);
                _position  += nbytes;
                _count     -= nbytes;
                if (_count == 0) {
                    _state = PREPARE_BLOCK;
                }
                break;
            }
        }
    }

    /**
     * Implementation of receive in mode E. There will be an instance
     * per data channel.
     */
    class Receiver extends AbstractMultiplexerListener
    {
        /** Socket used for data channel. */
        protected final SocketChannel _socket;

        /** Number of bytes left of current block. */
        protected long          _count;

        /** The file position at which we will receive data next. */
        protected long          _position;

        /** Header flags from the current block. */
        protected int           _flags;

        /** True if any data has flown over this data channel. */
        protected boolean       _used;

        /** Buffer for receiving the block header. */
        protected final ByteBuffer _header =
            ByteBuffer.allocate(HEADER_LENGTH);

        public Receiver(SocketChannel socket) {
            _socket   = socket;
            _count    = 0;
            _position = 0;
            _flags    = 0;
            _used     = false;
        }

        @Override
        public void register(Multiplexer multiplexer) throws IOException
        {
            multiplexer.register(this, SelectionKey.OP_READ, _socket);
        }

        @Override
        public void read(Multiplexer multiplexer, SelectionKey key)
                throws IOException, FTPException
        {
            try {
                doRead(multiplexer, key);
            } catch (IOException | FTPException e) {
                _activeDataChannels.decrementAndGet();
                _errorDataChannels.increment();
                _lastError = messageOrClassName(e);
                throw e;
            }
        }

        private void doRead(Multiplexer multiplexer, SelectionKey key)
                throws IOException, FTPException
        {
            /* _count is zero when we have received all of the
             * previous block. We expect to read the header of the
             * next block.
             */
            if (_count == 0) {
                long nbytes = _socket.read(_header);
                if (nbytes == -1) {
                    /* Stream was closed. The GridFTP 1 spec states
                     * that the sender must send EOD when no more data
                     * is to be send on a channel. However, the Globus
                     * GridFTP client library seems to consider it
                     * acceptable to close a data channel without
                     * sending EOD as long as it has not transferred
                     * any data on the channel. (REVISIT: Is this
                     * actually the case?)
                     */
                    if (_used) {
                        throw new FTPException("Stream ended before EOD");
                    }
                    close(multiplexer, key, _opened == _eodc);
                    _activeDataChannels.decrementAndGet();
                    return;
                }

                _used = true;

                if (_header.position() < _header.limit()) {
                    /* Incomplete header.
                     */
                    return;
                }

                _header.rewind();
                _flags    = _header.get();
                _count    = _header.getLong();
                _position = _header.getLong();
                _header.clear();

                /* The GridFTP spec states that we should generate an
                 * error whenever we receive a descriptor we don't
                 * know how to handle.
                 */
                if ((_flags & ~KNOWN_DESCRIPTORS) != 0) {
                    throw new FTPException("Received block with unknown descriptor (" + _flags + ")");
                }

                /* Exactly one EOF must be received on one of the data
                 * channels. It contains the number of EOD markers
                 * that must be received. Such blocks have a different
                 * interpretation of the two fields following the
                 * descriptor. Therefore these blocks cannot contain data.
                 *
                 * The GridFTP spec is not clear about EOF being
                 * received after EOD. As far as I can see, there
                 * would be a race condition if EOF after EOD were
                 * allowed when caching data channels: We have two
                 * channels A and B. We send EOD on both, and then
                 * EOF(2) on A. Now sender considers the transfer to
                 * be completed and initiates a new transfer on cached
                 * data channels A and B, however the file is so small
                 * that only an EOF is sent on B. It may now happen
                 * that the receiver sees an EOF on both channels and
                 * is not able to distinguish them. I therefore
                 * consider EOF after EOD to be disallowed. REMARK: We
                 * do not support caching of data channels.
                 *
                 * The GridFTP spec is also not clear about data being
                 * send after EOF, however we handle that case.
                 */
                if ((_flags & EOF_DESCRIPTOR) != 0) {
                    if (_eodc != 0) {
                        throw new FTPException("Multible EODC received");
                    }
                    if (_position <= 0) {
                        throw new FTPException("Non-positive EODC received");
                    }
                    _eodc = (int)_position;
                    _count = _position = 0; // No data
                }


                /* transferFrom does not like empty reads. Therefore
                 * we exit early in case of empty blocks.
                 */
                if (_count == 0) {
                    /* If EOD was received, then close channel.
                     */
                    if ((_flags & EOD_DESCRIPTOR) != 0) {
                        close(multiplexer, key, _opened == _eodc);
                        _activeDataChannels.decrementAndGet();
                    }
                    return;
                }

            }

            /* Receive data.
             */
            long nbytes = transferFrom(_socket, _position, _count);
            if (nbytes == -1) {
                throw new FTPException("Stream was closed in the middle of a block");
            }
            _monitor.receivedBlock(_position, nbytes);
            _position += nbytes;
            _count    -= nbytes;

            /* If EOD was received, then close channel.
             */
            if (_count == 0 && (_flags & EOD_DESCRIPTOR) != 0) {
                close(multiplexer, key, _opened == _eodc);
                _activeDataChannels.decrementAndGet();
            }
        }
    }

    public ModeE(Role role, RepositoryChannel file, ConnectionMonitor monitor,
                 int blockSize)
        throws IOException
    {
        super(role, file, monitor);
        _currentPosition = getStartPosition();
        _currentCount    = getSize();
        _eodc            = 0;
        _blockSize       = blockSize;
    }

    @Override
    public void setPartialRetrieveParameters(long position, long size)
    {
        super.setPartialRetrieveParameters(position, size);
        _currentPosition = getStartPosition();
        _currentCount    = getSize();
    }

    @Override
    public void newConnection(Multiplexer multiplexer, SocketChannel socket)
        throws IOException
    {
        _transferStarted = true;
        _activeDataChannels.incrementAndGet();
        switch (_role) {
        case Sender:
            multiplexer.add(new Sender(socket));
            break;
        case Receiver:
            multiplexer.add(new Receiver(socket));
            break;
        }
    }

    @Override
    public String name()
    {
        return "E (Extended)";
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        super.getInfo(pw);

        switch (_direction) {
        case Incomming:
            pw.println("EOF flag: " + (_eodc == 0 ? "not received" : "received"));
            if (_eodc > 0) {
                pw.println("Expected streams: " + _eodc);
            }
            break;

        case Outgoing:
            String percent = getSize() > 0
                    ? (" (" + toThreeSigFig(100 * _currentCount / (double)getSize(), 1000) + "% desired transfer)")
                    : "";
            pw.println("Bytes still to send: " + describeSize(_currentCount) + percent);
            pw.println("Offset of next send block: " + describeSize(_currentPosition));
            break;
        }

        if (_lastError != null) {
            pw.println("Last error: " + _lastError);
        }
    }

    @Override
    public boolean hasCompletedSuccessfully()
    {
        return _transferStarted && _activeDataChannels.get() == 0 && _errorDataChannels.longValue() == 0;
    }
}
