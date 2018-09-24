package org.dcache.ftp.data;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.dcache.pool.repository.RepositoryChannel;

import static org.dcache.util.Exceptions.messageOrClassName;

/** Implementation of MODE S. */
public class ModeS extends Mode
{
    private final int _blockSize;
    private volatile boolean _transferStarted;
    private volatile boolean _transferCompleted;
    private volatile boolean _transferFailed;
    private String _lastError;

    /* Implements MODE S send operation. */
    private class Sender extends AbstractMultiplexerListener
    {
        protected final SocketChannel _socket;
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
                throws IOException, FTPException
        {
            try {
                doWrite(multiplexer, key);
            } catch (IOException | FTPException e) {
                _transferFailed = true;
                _lastError = messageOrClassName(e);
                throw e;
            }
        }

        private void doWrite(Multiplexer multiplexer, SelectionKey key)
            throws IOException, FTPException
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
                _transferCompleted = true;
            }
        }
    }

    /* Implements MODE S receive operation. */
    private class Receiver extends AbstractMultiplexerListener
    {
        protected final SocketChannel _socket;
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
                throws IOException, InterruptedException, FTPException
        {
            try {
                doRead(multiplexer, key);
            } catch (IOException | FTPException e) {
                _transferFailed = true;
                _lastError = messageOrClassName(e);
                throw e;
            }
        }

        private void doRead(Multiplexer multiplexer, SelectionKey key)
                throws IOException, FTPException
        {
            long nbytes = transferFrom(_socket, _position, _blockSize);
            if (nbytes == -1) {
                close(multiplexer, key, true);
                _transferCompleted = true;
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
        throws IOException
    {
        _transferStarted = true;
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
    public void getInfo(PrintWriter pw)
    {
        super.getInfo(pw);
        if (_lastError != null) {
            pw.println("Last error: " + _lastError);
        }
    }

    @Override
    public String name()
    {
        return "S (Stream)";
    }

    @Override
    public boolean hasCompletedSuccessfully()
    {
        return _transferStarted && _transferCompleted && !_transferFailed;
    }
}
