package org.dcache.ftp.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.regex.Pattern;

import org.dcache.pool.repository.RepositoryChannel;

/**
 * Implementation of MODE X.
 *
 */
public class ModeX extends Mode
{
    enum SenderState
    {
        /** Wait for receiver to send READY. */
        WAIT_READY,

        /** Wait for receiver to send BYE. */
        WAIT_BYE,

        /** Prepare next block for transmission. */
        NEXT_BLOCK,

        /** Send the block header. */
        SEND_HEADER,

        /** Send the block data. */
        SEND_DATA
    }

    enum ReceiverState
    {
        /** Send BYE message. */
        SEND_BYE,

        /** Read block header. */
        READ_HEADER,

        /** Read block data. */
        READ_DATA
    }

    /**
     * Header length of a mode E block.
     */
    public static final int HEADER_LENGTH = 25;

    public static final int EOF_DESCRIPTOR                       = 64;
    public static final int EOD_DESCRIPTOR                       = 8;
    public static final int SENDER_CLOSES_THIS_STREAM_DESCRIPTOR = 4;

    public static final int KNOWN_DESCRIPTORS =
            EOF_DESCRIPTOR | EOD_DESCRIPTOR | SENDER_CLOSES_THIS_STREAM_DESCRIPTOR;

    /**
     * The chunk size used when sending files.
     *
     * Large blocks will reduce the overhead of sending. However, in
     * case of multible concurrent streams, large blocks will make
     * disk access less sequential on both the sending and receiving
     * side.
     */
    private final int _blockSize;

    /**
     * Position in file when sending data. Used by Sender.
     */
    private long _currentPosition;

    /**
     * Number of not yet transferred. Used by Sender.
     */
    private long _currentCount;

    /**
     * True iff EOF was sent or received.
     */
    private boolean _eof;

    /**
     * Count how many EODs have been sent. Used by the sender to
     * ensure that we maintain at least one data channel until the
     * transfer has completed.
     */
    private int _closing;

    /**
     *
     */
    private static Charset _ascii = Charset.forName("ascii");

    /**
     *
     */
    private static CharsetEncoder _encoder = _ascii.newEncoder();

    /**
     *
     */
    private static CharsetDecoder _decoder = _ascii.newDecoder();

    /**
     * Implementation of send in mode X. There will be an instance per
     * data channel. The sender repeatedly bites _blockSize bytes of
     * the file and transfers it as a single block. I.e.
     * _currentPosition is incremented by _blockSize bytes at a time.
     */
    private class Sender extends AbstractMultiplexerListener
    {
        /** The data channel. */
        protected SocketChannel _socket;

        /** Current state of the sender. */
        protected SenderState   _state;

        /** Current file position from which we send. */
        protected long          _position;

        /** Number of bytes left to send from the current block. */
        protected long          _count;

        /** True if receiver has requested the channel to be closed. */
        protected boolean       _closeAtNextBlock;

        /** Buffer for representing a block header. */
        protected ByteBuffer _header =
                ByteBuffer.allocate(HEADER_LENGTH);

        /** Buffer for reading commands from the receiver. */
        protected ByteBuffer _command =
                ByteBuffer.allocate(128);

        /** Buffer for reading commands from the receiver. */
        protected CharBuffer _decodedCommand =
                CharBuffer.allocate(128);

        public Sender(SocketChannel socket)
        {
            _socket           = socket;
            _state            = SenderState.WAIT_READY;
            _closeAtNextBlock = false;
        }

        @Override
        public void register(Multiplexer multiplexer) throws IOException
        {
            multiplexer.register(this, SelectionKey.OP_READ, _socket);
        }

        @Override
        public void read(Multiplexer multiplexer, SelectionKey key)
                throws Exception
        {
            /* Protect against clients sending large commands. We
             * could enlarge the command buffer, but this would open
             * the server to DOS attacks from clients sending very
             * large commands.
             */
            if (!_command.hasRemaining()) {
                throw new FTPException("Command buffer full");
            }

            /* Read available data.
             */
            long nbytes = _socket.read(_command);
            if (nbytes == -1) {
                if (_state == SenderState.WAIT_READY) {
                    /* From the GridFTP v2 spec: "Passive receiver may
                     * close new data socket without sending 'READY'
                     * message or even stop accepting new
                     * connections."
                     *
                     * We extend this to also cover active receivers.
                     */
                    close(multiplexer, key, _eof);
                    return;
                } else {
                    throw new FTPException("Lost connection");
                }
            }

            /* Decode buffer.
             */
            _command.flip();
            _decoder.decode(_command, _decodedCommand, false);
            _command.compact();

            /* Remove first line from buffer.
             */
            char c;
            StringBuffer line = new StringBuffer();
            _decodedCommand.flip();
            do {
                /* Return early if command is incomplete.
                 */
                if (!_decodedCommand.hasRemaining()) {
                    _decodedCommand.limit(_decodedCommand.capacity());
                    return;
                }
                c = _decodedCommand.get();
                line.append(c);
            } while (c != '\n');
            _decodedCommand.compact();

            /* Split line into arguments.
             */
            String[] arg = Pattern.compile("\\s").split(line);
            if (arg.length == 0) {
                throw new FTPException("Empty command received (protocol violation)");
            }

            /* Interpret command.
             */
            String cmd = arg[0];
            if (cmd.equals("READY") && _state == SenderState.WAIT_READY) {
                _state = SenderState.NEXT_BLOCK;
                key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            } else if (cmd.equals("BYE") && _state == SenderState.WAIT_BYE) {
                // shut down
                close(multiplexer, key, _eof);
            } else if (cmd.equals("CLOSE")) {
                // shutdown the channel at the end of the current block
                _closeAtNextBlock = true;
            } else if (cmd.equals("RESEND")) {
                // resend a block
                throw new FTPException("RESEND is not implemented");
            } else {
                throw new FTPException("Unexpected command '" + cmd
                                               + "' in state " + _state);
            }
        }

        @Override
        public void write(Multiplexer multiplexer, SelectionKey key)
                throws Exception
        {
            switch (_state) {
            case NEXT_BLOCK:
                _position         = _currentPosition;
                _count            = Math.min(_currentCount, _blockSize);

                /* Prepare header.
                 */
                byte descriptor;
                if (_count == 0) {
                    // No more data. Send EOD and EOF.
                    descriptor =
                            (byte)(EOF_DESCRIPTOR
                                    | EOD_DESCRIPTOR
                                    | SENDER_CLOSES_THIS_STREAM_DESCRIPTOR);
                    _closing++;
                    _eof = true;
                } else if (_closeAtNextBlock && _opened > _closing + 1) {
                    // Receiver requested close. Notice that we only
                    // honor the close request as long as at least one
                    // data channel remains open.
                    descriptor =
                            (byte)(EOD_DESCRIPTOR
                                    | SENDER_CLOSES_THIS_STREAM_DESCRIPTOR);
                    _closing++;
                    _count = 0;
                } else {
                    // Regular block.
                    descriptor        = 0;
                    _currentPosition += _count;
                    _currentCount    -= _count;
                }

                _header.clear();
                _header.put(descriptor);
                _header.putLong(_count);
                _header.putLong(_position);
                _header.putLong(0);              // Transaction ID
                _header.flip();
                _state = SenderState.SEND_HEADER;

            case SEND_HEADER:
                _socket.write(_header);
                if (_header.position() < _header.limit()) {
                    break;
                }

                /* If at end of stream, stop subscription for write
                 * events and wait for BYE from receiver.
                 */
                if (_count == 0) {
                    key.interestOps(SelectionKey.OP_READ);
                    _state = SenderState.WAIT_BYE;
                    break;
                }
                _state = SenderState.SEND_DATA;

            case SEND_DATA:
                long nbytes = transferTo(_position, _count, _socket);
                _monitor.sentBlock(_position, nbytes);
                _position  += nbytes;
                _count     -= nbytes;
                if (_count == 0) {
                    _state = SenderState.NEXT_BLOCK;
                }
                break;
            }
        }
    }

    class Receiver extends AbstractMultiplexerListener
    {
        /** The data channel. */
        protected SocketChannel _socket;

        /** Current state of the receiver. */
        protected ReceiverState _state;

        /** Current position in file. */
        protected long          _position;

        /** Number of bytes left to receive from the current block. */
        protected long          _count;

        /** The flags of the last block header. */
        protected int           _flags;

        /** Buffer for representing a block header. */
        protected ByteBuffer _header =
                ByteBuffer.allocate(HEADER_LENGTH);

        /** Buffer for reading commands from the receiver. */
        protected ByteBuffer _command =
                ByteBuffer.allocate(128);

        public Receiver(SocketChannel socket)
        {
            _socket   = socket;
            _count    = 0;
            _position = 0;
            _flags    = 0;
            _state    = ReceiverState.READ_HEADER;
            _command.limit(0);
        }

        private void addCommand(String s)
        {
            CharBuffer buffer = CharBuffer.allocate(s.length() + 1);
            buffer.put(s);
            buffer.put('\n');
            buffer.flip();

            _command.compact();
            _encoder.encode(buffer, _command, true);
            _command.flip();
        }

        @Override
        public void register(Multiplexer multiplexer) throws IOException
        {
            multiplexer.register(this, SelectionKey.OP_WRITE, _socket);
            addCommand("READY");
        }

        @Override
        public void write(Multiplexer multiplexer, SelectionKey key)
                throws Exception
        {
            try {
                _socket.write(_command);
                if (_command.position() == _command.limit()) {
                    if (_state == ReceiverState.SEND_BYE) {
                        close(multiplexer, key, true); // TODO: Check true
                    } else {
                        key.interestOps(SelectionKey.OP_READ);
                    }
                }
            } catch (ClosedChannelException e) {
                /* From GridFTP v2 spec: "...the sender may choose not
                 * to wait for 'BYE' acknowledgement. The sender is
                 * allowed to close data channels immediately after
                 * sending EOD, and the receiver may get a socket
                 * error trying to send 'BYE' message back to the
                 * sender".
                 */
                if (_state == ReceiverState.SEND_BYE) {
                    close(multiplexer, key, true); // TODO: Check true
                } else {
                    throw e;
                }
            }
        }

        @Override
        public void read(Multiplexer multiplexer, SelectionKey key)
                throws Exception
        {
            long nbytes;

            switch (_state) {
            case READ_HEADER:
                /* Read header.
                 */
                nbytes = _socket.read(_header);
                if (nbytes == -1) {
                    /* Stream was closed. A sender must always send
                     * EOD on a channel before closing it. We
                     * therefore consider the end of stream to be an
                     * error.
                     */
                    throw new FTPException("Stream ended before EOD");
                }

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

                /* At least one EOF must be received. The transfer has
                 * been complete when EOF was received and all open
                 * channels have been closed.
                 */
                if ((_flags & EOF_DESCRIPTOR) != 0) {
                    _eof = true;
                }

                /* Empty blocks are allowed.
                 */
                if (_count == 0) {
                    /* If EOD was received then send BYE message.
                     */
                    if ((_flags & EOD_DESCRIPTOR) != 0) {
                        key.interestOps(SelectionKey.OP_WRITE);
                        addCommand("BYE");
                        _state = ReceiverState.SEND_BYE;
                    }
                    break;
                }

                _monitor.preallocate(_position + _count);
                _state = ReceiverState.READ_DATA;

            case READ_DATA:
                /* Receive data.
                 */
                nbytes = transferFrom(_socket, _position, _count);
                if (nbytes == -1) {
                    throw new FTPException("Stream was closed in the middle of a block");
                }
                _monitor.receivedBlock(_position, nbytes);
                _position += nbytes;
                _count    -= nbytes;

                if (_count == 0) {
                    /* If EOD was received then send BYE message.
                     */
                    if ((_flags & EOD_DESCRIPTOR) != 0) {
                        key.interestOps(SelectionKey.OP_WRITE);
                        addCommand("BYE");
                        _state = ReceiverState.SEND_BYE;
                    } else {
                        _state = ReceiverState.READ_HEADER;
                    }
                }
                break;
            }
        }
    }

    public ModeX(Role role, RepositoryChannel file, ConnectionMonitor monitor,
                 int blockSize)
            throws IOException
    {
        super(role, file, monitor);
        _currentPosition = getStartPosition();
        _currentCount    = getSize();
        _eof             = false;
        _closing         = 0;
        _blockSize = blockSize;
    }

    @Override
    public void newConnection(Multiplexer multiplexer, SocketChannel socket)
            throws Exception
    {
        switch (_role) {
        case Sender:
            multiplexer.add(new Sender(socket));
            break;
        case Receiver:
            /* From the GridFTP 2 spec: "After receiving EOF block
             * from a sender host, active data receiver host must not
             * try to open any new data channels to that sender host."
             *
             * This rule is difficult to honor, as we may have a
             * connection establishment "in progress" at the time we
             * received the EOF. At the same time it is not clear if
             * an active receiver is allowed to close a connection
             * before READY has been sent; passive receivers are
             * explicitly allowed to do so.
             */
            if (_eof) {
                socket.close();
            } else {
                multiplexer.add(new Receiver(socket));
            }
            break;
        }
    }
}
