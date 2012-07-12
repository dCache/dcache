package org.dcache.ftp;

import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.ClosedChannelException;
import java.io.IOException;
import org.dcache.pool.repository.RepositoryChannel;

/** Implementation of MODE S. */
public class ModeS extends Mode
{
    private final int _blockSize;

    /* Implements MODE S send operation. */
    private class Sender extends AbstractMultiplexerListener
    {
        protected SocketChannel _socket;
        protected long _position;
        protected long _count;

        public Sender(SocketChannel socket)
        {
            _socket   = socket;
            _position = getStartPosition();
            _count    = getSize();
        }

        @Override
        public void register(Multiplexer multiplexer) throws IOException
        {
            multiplexer.register(this, SelectionKey.OP_WRITE, _socket);
        }

        @Override
        public void write(Multiplexer multiplexer, SelectionKey key)
            throws Exception
        {
            long nbytes = transferTo(_position, _count, _socket);
            _monitor.sentBlock(_position, nbytes);

            _position += nbytes;
            _count    -= nbytes;

            /* There is no special end-of-file signal in mode S. Just
             * close the connection.
             */
            if (_count == 0) {
                close(multiplexer, key, true);
            }
        }
    }

    /* Implements MODE S receive operation. */
    private class Receiver extends AbstractMultiplexerListener
    {
        protected SocketChannel _socket;
        protected long          _position;

        public Receiver(SocketChannel socket)
        {
            _socket   = socket;
            _position = 0;
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
            _monitor.preallocate(_position + _blockSize);
            long nbytes = transferFrom(_socket, _position, _blockSize);
            if (nbytes == -1) {
                close(multiplexer, key, true);
            } else {
                _monitor.receivedBlock(_position, nbytes);
                _position += nbytes;
            }
        }
    }

    public ModeS(Role role, RepositoryChannel file, ConnectionMonitor monitor,
                 int blockSize)
        throws IOException
    {
        super(role, file, monitor);
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
            multiplexer.add(new Receiver(socket));
            break;
        }
    }
}
