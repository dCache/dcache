package org.dcache.ftp.data;

import com.google.common.collect.Iterables;
import com.google.common.net.InetAddresses;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.dcache.pool.repository.RepositoryChannel;

/**
 * Base class for FTP transfer mode implementations.
 *
 * A mode may make use of several connections at the same time. The
 * transfer will be coordinated by the mode object. Therefore, the
 * mode object knows about the file to transfer and the direction of
 * the transfer.
 */
public abstract class Mode extends AbstractMultiplexerListener
{
    protected Role              _role;
    protected Direction         _direction;
    protected RepositoryChannel       _file;
    protected ConnectionMonitor _monitor;

    private   long              _position;

    private   long              _size;

    private   long              _fileSize;

    /** Buffer for transferTo and transferFrom. */
    private   ByteBuffer        _buffer = ByteBuffer.allocate(8192);

    /** The address to connect to for outgoing connections. */
    private   InetSocketAddress     _address;

    /** The channel used for incomming connections. */
    private   ServerSocketChannel _channel;

    /** Size of send and recv buffer when larger than 0. */
    private   int               _bufferSize;

    /** The largest number of concurrent connections to accept. */
    protected int               _parallelism = 1;

    /** Disabled keys. The value is the interest set of the key. */
    protected Map<SelectionKey, Integer> disabled
        = new HashMap<>();

    /** Number of connections for which connect failed. */
    protected int               _failed;

    /** Number of connections that have been opened. */
    protected int               _opened;

    /** Number of connections that have been closed. */
    protected int               _closed;

    /** Remote addresses of data channels connected by this class. */
    private final Set<InetSocketAddress> _remoteAddresses = new HashSet<>();

    /** Constructs a new mode for outgoing connections. */
    public Mode(Role role, RepositoryChannel file, ConnectionMonitor monitor)
        throws IOException
    {
        _fileSize    = file.size();
        _role        = role;
        _file        = file;
        _size        = _fileSize;
        _monitor     = monitor;
    }

    /**
     * Enable passive mode. Connections will be accepted on the given
     * channel.
     */
    public void setPassive(ServerSocketChannel channel)
    {
        assert _address == null && _channel == null && channel != null;

        _direction = Direction.Incomming;
        _channel   = channel;
    }

    /**
     * Enable active mode. Connections will be made to the given
     * address.
     *
     * @throws UnresolvedAddressException if the address is unresolved
     */
    public void setActive(InetSocketAddress address)
        throws UnresolvedAddressException
    {
        assert _address == null && _channel == null && address != null;

        if (address.isUnresolved()) {
            throw new UnresolvedAddressException();
        }

        _direction = Direction.Outgoing;
        _address   = address;
    }

    /**
     * Set parameters for partial retrive. This makes only sense when
     * the role is Role.Sender.
     */
    public void setPartialRetrieveParameters(long position, long size)
    {
        if (_position < 0 || size < 0 || position + size > _fileSize) {
            throw new IllegalArgumentException();
        }
        _position = position;
        _size     = size;
    }

    /**
     * Set socket buffer size. The same value is used for send and
     * receive buffers. A value of zero enables auto tuning. Auto
     * tuning is enabled by default.
     */
    public void setBufferSize(int value)
    {
        if (value < 0) {
            throw new IllegalArgumentException("Buffer size must be non-negative");
        }
        _bufferSize = value;
    }

    /**
     * Sets the number of concurrent connections to use. Only relevant
     * for outgoing connections. Parallelism is not supported by all
     * modes.
     */
    public void setParallelism(int value)
    {
        if (value <= 0) {
            throw new IllegalArgumentException("Parallelism must be positive");
        }
        _parallelism = value;
    }

    /** Returns the starting position of the transfer. */
    public long getStartPosition()
    {
        return _position;
    }

    /** Returns the number of bytes to transfer. */
    public long getSize()
    {
        return _size;
    }

    /** Returns the remote addresses the mode connected with. */
    public Collection<InetSocketAddress> getRemoteAddresses()
    {
        return Collections.unmodifiableCollection(_remoteAddresses);
    }

    /**
     * Like calling _file.transferTo().
     *
     * This method behaves similarly to FileChannel.transferTo, except
     * that it never uses zero-copy mode. FileChannel.transferTo has
     * been subject to a large number of bugs throughout the history
     * of Java.
     */
    protected long transferTo(long position, long count, SocketChannel socket)
        throws IOException
    {
        long tr = 0;                        // Total bytes read
        long pos = position;
        _buffer.clear();
        while (tr < count) {
            _buffer.limit((int)Math.min((count - tr),
                                        (long)_buffer.capacity()));
            int nr = _file.read(_buffer, pos);
            if (nr < 0 && tr == 0) {
                return -1;
            }
            if (nr <= 0) {
                break;
            }
            _buffer.flip();
            int nw = socket.write(_buffer);
            tr += nw;
            if (nw != nr) {
                break;
            }
            pos += nw;
            _buffer.clear();
        }
        return tr;
    }

    /**
     * Similar to _file.transferFrom(). In contrast to
     * FileChannel.transferFrom(), this method does detect
     * end-of-stream and returns -1 in that case.
     *
     * Originally, this method was based on
     * FileChannel.transferFrom(), but spurious behaviour was observed
     * in some cases (transferFrom returning 0, even though the
     * selector claimed data was ready and a normal read returned
     * data).
     *
     * The current implementation copies data into memory and writes
     * it do disk. This should be no slower than using
     * FileChannel.transferFrom() from JDK 6, since that does exactly
     * the same when copying from a SocketChannel.
     *
     * An alternative would be to map the file into memory and read
     * from the socket directly into the mapped file. That however
     * would be better done at a higher level and it is currently
     * unknown if the performance would improve.
     */
    protected long transferFrom(SocketChannel socket, long position, long count)
        throws IOException
    {
        long tw = 0;                    // Total bytes written
        long pos = position;
        try {
            _buffer.clear();
            while (tw < count) {
                _buffer.limit((int)Math.min((count - tw),
                                            (long)_buffer.capacity()));
                int nr = socket.read(_buffer);
                if (nr < 0 && tw == 0) {
                    return -1;
                }
                if (nr <= 0) {
                    break;
                }
                _buffer.flip();
                int nw = _file.write(_buffer, pos);
                tw += nw;
                if (nw != nr) {
                    break;
                }
                pos += nw;
                _buffer.clear();
            }
            return tw;
        } catch (IOException x) {
            if (tw > 0) {
                return tw;
            }
            throw x;
        }
    }

    /**
     * Register the mode for outgoing connections. One or more
     * connections will be established asynchronously. The number of
     * connections to create is controlled by the parallelism.
     *
     * An IOException may be thrown if all connections attempts
     * fail. Failures to create a SocketChannel are propagated to the
     * caller.
     *
     * @see setParallelism(), SocketChannel.open()
     */
    protected void registerOutgoing(Multiplexer multiplexer)
        throws Exception
    {
        IOException lastException = null;

        for (int i = 0; i < _parallelism; i++) {
            /* Errors in socket channel creation are likely to
             * indicate some serious problems. Therefore we let the
             * caller figure out what to do (i.e. we do not catch the
             * exception).
             */
            SocketChannel channel = SocketChannel.open();
            try {
                channel.configureBlocking(false);
                if (_bufferSize > 0) {
                    channel.socket().setReceiveBufferSize(_bufferSize);
                    channel.socket().setSendBufferSize(_bufferSize);
                }
                channel.socket().setKeepAlive(true);

                SelectionKey key =
                    multiplexer.register(this, SelectionKey.OP_CONNECT, channel);

                multiplexer.say("Connecting to " + _address);
                if (channel.connect(_address)) {
                    connect(multiplexer, key);
                }
            } catch (IOException e) {
                SocketAddress remoteAddress = channel.getRemoteAddress();

                // Any error is logged, but otherwise ignored.  As
                // long as at least one connection succeeds, the
                // transfer can be completed.
                channel.close();
                lastException = e;
                String displayAddress;
                if (remoteAddress instanceof InetSocketAddress) {
                    InetSocketAddress ia = (InetSocketAddress) remoteAddress;
                    displayAddress = InetAddresses.toUriString(ia.getAddress()) + ":" + ia.getPort();
                } else {
                    displayAddress = remoteAddress.toString();
                }
                multiplexer.esay("Problem with " + displayAddress + ": " + e.getMessage());
                _failed++;

                if (allConnectionsEstablished()) {
                    enableDisabledKeys();
                }
            }
        }

        if (_failed == _parallelism) {
            throw lastException;
        }
    }

    public String getRemoteAddressDescription()
    {
        switch (_direction) {
        case Outgoing:
            if (_address == null) {
                return null;
            }
            return InetAddresses.toUriString(_address.getAddress()) + ":" + _address.getPort();

        case Incomming:
            Set<String> addresses = new HashSet<>();
            for (InetSocketAddress addr : _remoteAddresses) {
                String description = InetAddresses.toUriString(addr.getAddress()) +
                        ":" + addr.getPort();
                addresses.add(description);
            }
            return addresses.size() == 1 ? Iterables.getOnlyElement(addresses) :
                    addresses.toString();
        }

        return null;
    }

    /**
     * Register the mode for incomming connections.
     */
    protected void registerIncomming(Multiplexer multiplexer)
        throws IOException
    {
        _channel.configureBlocking(false);
        multiplexer.say("Accepting connections on " +
                        _channel.socket().getLocalSocketAddress());
        multiplexer.register(this, SelectionKey.OP_ACCEPT, _channel);
    }

    /**
     * Registers this mode with a multiplexer.
     */
    @Override
    public void register(Multiplexer multiplexer)
        throws Exception
    {
        assert _address != null || _channel != null
            : "Mode must be either set to passive or active.";

        switch (_direction) {
        case Incomming:
            registerIncomming(multiplexer);
            break;
        case Outgoing:
            registerOutgoing(multiplexer);
            break;
        default:
            // Ignore
            break;
        }
    }

    /**
     * Called by the multiplexer when a new incomming connection can
     * be accepted. A new socket is created and newConnection() is
     * called.
     *
     * Failure to accept the connection is propagated to the caller.
     */
    @Override
    public void accept(Multiplexer multiplexer, SelectionKey key)
        throws Exception
    {
        ServerSocketChannel server = (ServerSocketChannel)key.channel();
        SocketChannel channel = server.accept();
        if (channel != null) {
            Socket socket = channel.socket();
            _opened++;
            multiplexer.say("Opened " + socket);
            _remoteAddresses.add((InetSocketAddress) socket.getRemoteSocketAddress());
            channel.configureBlocking(false);
            if (_bufferSize > 0) {
                channel.socket().setSendBufferSize(_bufferSize);
            }
            channel.socket().setKeepAlive(true);
            newConnection(multiplexer, channel);
        }
    }

    /**
     * Called by the multiplexer when a new outgoing connection has
     * been established. If all outgoing connections have been
     * established or failed, then all keys disabled by
     * waitForConnectionCompletion() are enabled.
     *
     * Propagates failures to finish the connection establishment to
     * the caller.
     */
    @Override
    public void connect(Multiplexer multiplexer, SelectionKey key)
        throws Exception
    {
        try {
            SocketChannel channel = (SocketChannel)key.channel();
            if (channel.finishConnect()) {
                Socket socket = channel.socket();
                _opened++;
                multiplexer.say("Opened " + socket);
                _remoteAddresses.add((InetSocketAddress) socket.getRemoteSocketAddress());
                newConnection(multiplexer, channel);
            }
        } catch (IOException e) {
            _failed++;
            if (_failed == _parallelism) {
                throw e;
            }
        } finally {
            if (allConnectionsEstablished()) {
                enableDisabledKeys();
            }
        }
    }

    /**
     * Close the socket channel associated with key.
     *
     * If mayShutdown is true and all connections have been closed,
     * then the multiplexer is shut down.
     */
    protected void close(Multiplexer multiplexer, SelectionKey key,
                         boolean mayShutdown)
        throws IOException
    {
        SocketChannel channel = (SocketChannel)key.channel();
        multiplexer.say("Closing " + channel.socket());

        key.cancel();
        channel.close();

        _closed++;
        if (mayShutdown && _closed == _opened) {
            multiplexer.shutdown();
        }
    }

    /**
     * Reestablishes notification for all disabled keys.
     *
     * @see disableKey
     */
    private void enableDisabledKeys()
    {
        for (Map.Entry<SelectionKey,Integer> e : disabled.entrySet()) {
            e.getKey().interestOps(e.getValue());
        }
        disabled.clear();
    }

    /**
     * Disables notification for a key.
     *
     * @see enableDisabledKeys
     */
    private void disableKey(SelectionKey key)
    {
        if (!disabled.containsKey(key)) {
            disabled.put(key, key.interestOps());
            key.interestOps(0);
        }
    }

    /**
     * Returns true iff all connections have been either established
     * or failed.
     */
    private boolean allConnectionsEstablished()
    {
        return (_opened + _failed >= _parallelism);
    }

    /**
     * Returns true if all connections have been established or
     * connection establishment has failed (wrt. the parallelism),
     * false otherwise. When false, the key is deactivated until
     * waitForConnectionCompletion would return true.
     *
     * This call is only valid if the direction of the mode is set to
     * Outgoing.
     */
    protected boolean waitForConnectionCompletion(SelectionKey key)
    {
        if (_direction != Direction.Outgoing) {
            throw new IllegalArgumentException("Call is only valid for outgoing connections");
        }
        if (allConnectionsEstablished()) {
            return true;
        }
        disableKey(key);
        return false;
    }

    /**
     * Called by a Connection object when a new connection has been
     * established.
     */
    abstract protected void newConnection(Multiplexer multiplexer,
                                          SocketChannel channel)
        throws Exception;

}


