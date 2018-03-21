/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.ftp.proxy;

import com.google.common.base.Supplier;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.ProtocolFamily;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.dcache.ftp.door.AbstractFtpDoorV1.Protocol;
import org.dcache.util.ByteUnit;
import org.dcache.util.PortRange;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.find;
import static java.util.Collections.synchronizedList;
import static org.dcache.util.ByteUnit.KiB;

/**
 * Takes responsibility for opening a ServerSocket and handling
 * connections to that socket.  Provides an accept-like method that can reuse
 * existing TCP connections.
 */
public class PassiveConnectionHandler implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PassiveConnectionHandler.class);
    private static final ByteBuffer NO_DATA = ByteBuffer.allocate(0).asReadOnlyBuffer();

    /**
     * A channel that has received data from a client while
     * PassiveConnectionHandler has state REAPER_ACTIVE or
     * ACCEPT_SINGLESHOT_REAPER_ACTIVE.  Returned channels
     * (see {@link #returnChannel(java.nio.channels.SocketChannel)) should not become active outside of a
     * transfer; however, there is a race-condition between dCache replying to
     * the client that an upload can proceed and calling
     * {@link #accept(java.util.function.BiConsumer)}
     * reacting to an upload command (STOR or PUT) and the client
     * Such channels are supplied to the
     * consumer (along with the data) after the transition to ACCEPT_WITH_REUSE
     * state.
     */
    private class ActiveChannel
    {
        private final SocketChannel channel;
        private final ByteBuffer activity;

        public ActiveChannel(SocketChannel channel, ByteBuffer activity)
        {
            this.channel = channel;
            this.activity = activity;
        }
    }

    /**
     * Possible states of this handler.  Supported transitions are:
     * <pre>
     *     +---------------------------------------------------------------+
     *     |                       +------------------+                    |
     *     v                       v                  |                    |
     * NO_SERVER_SOCKET ---> SOCKET_OPENED -+-> ACCEPT_SINGLESHOT ------->-+
     *                                      |                              |
     *                                      +-> ACCEPT_WITH_REUSE ------->-+
     *                                                ^    |               |
     *                                                |    v               |
     *                                            REAPER_ACTIVE --------->-+
     *                                                ^    |               |
     *                                                |    v               |
     *                                  ACCEPT_SINGLESHOT_REAPER_ACTIVE ->-+
     * </pre>
     */
    private enum State
    {
        /** No server socket is opened. */
        NO_SERVER_SOCKET,

        /** The server socket is opened but not listening for connections. */
        SOCKET_OPENED,

        /**
         * The server socket is open and waiting for a single incoming
         * connection.  Any established data connection is not returned, even if
         * it becomes active.
         */
        ACCEPT_SINGLESHOT,

        /**
         * The server socket is open and accepting incoming connections.
         * Established connections that become active are presented as if they
         * were fresh connections.
         */
        ACCEPT_WITH_REUSE,

        /**
         * The server socket is open but without processing data.  If the
         * remote party closes their half of an idle connection then dCache
         * closes the corresponding half (so fully closing the connection).  If
         * the client sends data in this state then it is queued.  Such data
         * will be presented when next in state ACCEPT_WITH_REUSE or discarded
         * if NO_SERVER_SOCKET.
         */
        REAPER_ACTIVE,

        /**
         * The server socket is open and waiting for a single incoming
         * connection.  Any established connections are automatically closed if
         * the remote party closes their half with any data queued.
         */
        ACCEPT_SINGLESHOT_REAPER_ACTIVE;
    }

    private final InetAddress _address;
    private final PortRange _portRange;
    private final List<SocketChannel> _activeChannels = synchronizedList(new ArrayList<>());
    private final List<SocketChannel> _idleChannels = synchronizedList(new ArrayList<>());
    private final List<SocketChannel> _toBeRegistered = synchronizedList(new ArrayList<>());
    private final List<ActiveChannel> _backgroundActiveChannels = synchronizedList(new ArrayList<>());
    private final AtomicInteger _singleUseAccepts = new AtomicInteger();
    private final AtomicInteger _reuseableAccepts = new AtomicInteger();
    private final AtomicInteger _connectionReuse = new AtomicInteger();
    private final Object _selectorLock = new Object();

    @GuardedBy("this")
    private State _state = State.NO_SERVER_SOCKET;

    @GuardedBy("this")
    private Selector _selector;

    @GuardedBy("_selectorLock")
    private ServerSocketChannel _channel;

    @GuardedBy("this")
    private Supplier<Iterable<InterfaceAddress>> _addressSupplier;

    @GuardedBy("this")
    private Protocol _preferredProtocol;

    @GuardedBy("this")
    private Consumer<String> _errorConsumer = (String s) -> {};

    @GuardedBy("this")
    private ByteBuffer _data;

    private volatile boolean _selectorFinishRequested;

    public PassiveConnectionHandler(InetAddress address, PortRange range)
    {
        _address = address;
        _portRange = range;
    }

    public InetSocketAddress getLocalAddress()
    {
        synchronized (_selectorLock) {
            return _channel == null ? null : (InetSocketAddress) _channel.socket().getLocalSocketAddress();
        }
    }

    public synchronized void setAddressSupplier(Supplier<Iterable<InterfaceAddress>> addressSupplier)
    {
        checkState(_state == State.NO_SERVER_SOCKET, "Cannot specify address supplier after socket opened.");
        _addressSupplier = addressSupplier;
    }

    /**
     * Specify the desired IP protocol.  Requires prior call to
     * {@code #setAddressSupplier}.  If this is called after {@code #open()}
     * then any existing TCP connections are closed, as is the ServerSocket
     * and a subsequent call to {@code #open()} is required.
     */
    public synchronized void setPreferredProtocol(Protocol preferred)
    {
        checkState(_addressSupplier != null, "No address supplier provided");
        _preferredProtocol = preferred;

        if (_state != State.NO_SERVER_SOCKET) {
            Protocol current;
            synchronized (_selectorLock) {
                current = Protocol.fromAddress(_channel.socket().getInetAddress());
            }

            if (current != preferred) {
                close();
            }
        }
    }

    public synchronized ProtocolFamily getPreferredProtocolFamily()
    {
        return _preferredProtocol.getProtocolFamily();
    }

    public synchronized void setErrorConsumer(Consumer<String> errorConsumer)
    {
        _errorConsumer = errorConsumer;
    }

    /**
     * Open a ServerSocket, based on the supplied preferred IP protocol
     * (if any) and desired port-range (if any).  This method may be called
     * multiple times: it only has effect if the first call or after close.
     */
    public synchronized void open() throws IOException
    {
        LOGGER.trace("open");

        if (_state == State.NO_SERVER_SOCKET) {
            InetAddress address = _address;
            if (_preferredProtocol != null && Protocol.fromAddress(address) != _preferredProtocol) {
                Iterable<InterfaceAddress> addresses = _addressSupplier.get();
                address = find(addresses, a -> Protocol.fromAddress(a.getAddress()).equals(_preferredProtocol))
                        .getAddress();
            }
            synchronized (_selectorLock) {
                _channel = ServerSocketChannel.open();
                _portRange.bind(_channel.socket(), address);
                LOGGER.debug("Server socket opened {}", _channel);
            }
            _state = State.SOCKET_OPENED;
        }
    }

    /**
     * Await for a TCP connection to be established.  The caller is
     * responsible for calling {@code Socket#close} on the returned value.  This
     * may not be called at the same time as the other accept method is active.
     */
    public SocketChannel accept() throws IOException
    {
        ServerSocketChannel channel;
        synchronized (this) {
            checkState(_state != State.NO_SERVER_SOCKET, "failed to call open");
            checkState(_state != State.ACCEPT_WITH_REUSE, "cannot call both accept methods at the same time");
            synchronized (_selectorLock) {
                channel = _channel;
            }
            _state = _state == State.SOCKET_OPENED ? State.ACCEPT_SINGLESHOT : State.ACCEPT_SINGLESHOT_REAPER_ACTIVE;
        }

        LOGGER.debug("Accepting output connection within ConnectionHandler on {}",
             channel.socket().getLocalSocketAddress());
        boolean isBlocking = channel.isBlocking();

        try {
            channel.configureBlocking(true);
            SocketChannel socket = channel.accept();

            _singleUseAccepts.incrementAndGet();

            socket.socket().setKeepAlive(true);
            return socket;
        } finally {
            synchronized (this) {
                _state = _state == State.ACCEPT_SINGLESHOT ? State.SOCKET_OPENED : State.REAPER_ACTIVE;
                synchronized (_selectorLock) {
                    if (_channel.isOpen()) {
                        _channel.configureBlocking(isBlocking);
                    }
                }
            }
        }
    }

    /**
     * Await activity on TCP connections.  TCP connections with activity
     * are presented to the {@literal channelConsumer} that takes
     * responsibility for handling the activity and subsequently returning the
     * channel via the {@code #returnChannel} method.  Active TCP connections
     * are either freshly established TCP connections or idle connections in
     * which the client has sent data.
     * <p>
     * The thread calling this method will invoke the
     * {@literal channelConsumer} accept method with active TCP connections
     * zero or more times.  The thread only returns after the
     * {@code #finishAccept} method is called.
     * <p>
     * The channel consumer is an object that will take responsibility for
     * processing received data and replying as appropriate.  The channel
     * consumer may close the connection, if appropriate.  The consumer must
     * return the channel once no further activity is expected, using the
     * {@code #returnChannel} method.  The consumer itself must not block
     * when presented with a channel.
     * <p>
     * After this method returns, any returned channel will be closed whenever
     * the remote party has closed their end of the connection.  To achieve
     * this, a background thread attempts to read data when the channel becomes
     * active.  Any data read by the background thread is presented along with
     * the channel at the next call to this method.
     * @param channelConsumer the object that will handle active channels.
     * @throws IOException if there is a problem while establishing connections.
     */
    public void accept(BiConsumer<SocketChannel,ByteBuffer> channelConsumer)
            throws IOException, InterruptedException
    {
        LOGGER.trace("accept");

        BiConsumer<SocketChannel,ByteBuffer> registeringChannelConsumer = (c,b) -> {
                    _activeChannels.add(c);
                    channelConsumer.accept(c, b);
                };

        synchronized (this) {
            checkState(_state != State.NO_SERVER_SOCKET, "open not called");
            checkState(_state != State.ACCEPT_WITH_REUSE, "accept already running");
            checkState(_state != State.ACCEPT_SINGLESHOT
                    && _state != State.ACCEPT_SINGLESHOT_REAPER_ACTIVE, "awaiting single-shot accept");

            if (_state == State.REAPER_ACTIVE) {
                stopSelectionLoop();
            }
            _state = State.ACCEPT_WITH_REUSE;

            LOGGER.debug("Accepting input connection on {}", getLocalAddress());

            synchronized (_selectorLock) {
                assert _selector == null;
                _selector = Selector.open();
                _channel.configureBlocking(false);
                _channel.register(_selector, SelectionKey.OP_ACCEPT);
                registerAll(_idleChannels);
                _selectorFinishRequested = false;
            }
        }

        drainTo(_backgroundActiveChannels, ac -> registeringChannelConsumer.accept(ac.channel, ac.activity));

        selectionLoop(c -> registeringChannelConsumer.accept(c, NO_DATA),
                c -> registeringChannelConsumer.accept(c, NO_DATA));
    }

    private void selectionLoop(Consumer<SocketChannel> newChannels,
            Consumer<SocketChannel> readableChannels) throws IOException
    {
        Selector selector;
        ServerSocketChannel serverChannel;
        synchronized (_selectorLock) {
            selector = _selector;
            serverChannel = _channel;
        }

        while (!Thread.currentThread().isInterrupted() && !_selectorFinishRequested) {
            selector.select();
            for (SelectionKey key : selector.selectedKeys()) {
                if (key.isAcceptable()) {
                    try {
                        SocketChannel newChannel = serverChannel.accept();
                        _reuseableAccepts.incrementAndGet();
                        try {
                            newChannel.socket().setKeepAlive(true);
                        } catch (IOException e) {
                            LOGGER.warn("Unable to set KeepAlive: {}", e.toString());
                        }

                        newChannels.accept(newChannel);
                    } catch (IOException e) {
                        LOGGER.warn("Failed to accept incoming connection: {}", e.toString());
                    }
                } else if (key.isReadable()) {
                    SocketChannel activeChannel = (SocketChannel) key.channel();
                    if (removeIdleChannel(activeChannel,
                            "Received SelectionKey isReadable event for non-idle channel")) {
                        _connectionReuse.incrementAndGet();
                        key.interestOps(0);
                        readableChannels.accept(activeChannel);
                    }
                }
            }
            selector.selectedKeys().clear();

            synchronized (_selectorLock) {
                drainTo(_toBeRegistered, c -> {
                            if (registerChannel(c)) {
                                _idleChannels.add(c);
                            } else {
                                closeChannel(c, "Defensive close failed after failure to register channel");
                            }
                        });
            }
        }

        synchronized (_selectorLock) {
            _selectorLock.notifyAll();
        }
    }

    private static <T> void drainTo(Collection<T> items, Consumer<T> consumer)
    {
        synchronized (items) {
            items.forEach(consumer);
            items.clear();
        }
    }

    /**
     * All elements in the collection are either registered in the selector or
     * are closed and removed from the collection.
     */
    @GuardedBy("_selectorLock")
    private void registerAll(Collection<SocketChannel> channels)
    {
        synchronized (channels) {
            Iterator<SocketChannel> itr = channels.iterator();
            while (itr.hasNext()) {
                SocketChannel channel = itr.next();
                if (!registerChannel(channel)) {
                    itr.remove();
                    closeChannel(channel, "Defensive close failed after failure to register channel");
                }
            }
        }
    }

    /**
     * Try to register a channel in the selector.  If a channel
     * is already registered then OP_READ selection is enabled.
     * @return true if channel is successfully registered.
     */
    @GuardedBy("_selectorLock")
    private boolean registerChannel(SelectableChannel channel)
    {
        try {
            channel.configureBlocking(false);
            SelectionKey key = channel.keyFor(_selector);
            if (key == null) {
                channel.register(_selector, SelectionKey.OP_READ);
            } else {
                key.interestOps(SelectionKey.OP_READ);
            }
            return true;
        } catch (ClosedChannelException | ClosedSelectorException e) {
            LOGGER.debug("Failed to register channel in selector: {}", e.toString());
        } catch (IOException e) {
            LOGGER.warn("Failed to register channel in selector: {}", e.toString());
        }
        return false;
    }

    /**
     * Register that no further activity is expected (in the immediate
     * future) for a given SocketChannel.  If the TCP connection
     * is left open by both ends then it is kept open for possible future use.
     * If the remote end has closed the the connection (e.g.,
     * SocketChannel#read returns -1) then SocketChannel#shutdownInput must be
     * called before returning the channel.
     */
    public synchronized void returnChannel(SocketChannel channel)
    {
        LOGGER.trace("returnChannel: {}", channel);

        if (_state == State.NO_SERVER_SOCKET || _state == State.SOCKET_OPENED
                || _state == State.ACCEPT_SINGLESHOT) {
            LOGGER.warn("Channel returning when in state {}", _state);
            tryCloseChannel(channel, "Defensive close due to unexpected state failed");
            return;
        }

        if (!_activeChannels.remove(channel)) {
            LOGGER.warn("returning channel that is not in active list.");
            tryCloseChannel(channel, "Defentive close due to unknown channel failed");
            return;
        }

        Socket s = channel.socket();
        if (s.isInputShutdown() || s.isOutputShutdown()) {
            closeChannel(channel, "Failed to close after remote party closed their half");
            return;
        }

        _toBeRegistered.add(channel);

        synchronized (_selectorLock) {
            _selector.wakeup();
        }
    }

    /**
     * Indicate that no further new connectors or idle-connectors becoming
     * active is expected.  If a thread is calling
     * {@code #accept(Consumer<SocketChannel>)} then calling this method
     * triggers it to return; no further calls to the supplied Consumer will be
     * made once this method returns.  If there is no such thread then this
     * method does nothing.
     */
    public synchronized void finishAccept() throws InterruptedException
    {
        LOGGER.debug("finishAccept");

        if (_state == State.ACCEPT_WITH_REUSE) {
            stopSelectionLoop();
            startConnectionReaper();
            _state = State.REAPER_ACTIVE;
        }
    }

    @GuardedBy("this")
    private void stopSelectionLoop() throws InterruptedException
    {
        assert _state == State.ACCEPT_WITH_REUSE
                    || _state == State.ACCEPT_SINGLESHOT_REAPER_ACTIVE
                    || _state == State.REAPER_ACTIVE;

        synchronized (_selectorLock) {
            _selectorFinishRequested = true;
            _selector.wakeup();
            _selectorLock.wait();

            try {
                _selector.close();
            } catch (IOException e) {
                LOGGER.error("Error closing selector: {}", e.toString());
            }
            _selector = null;
        }
    }

    /**
     * Close the channel.  A failure is reported back to the error consumer.
     * @param channel the channel to close.
     */
    @GuardedBy("this")
    private void closeChannel(Channel channel, String message)
    {
        try {
            channel.close();
        } catch (IOException e) {
            _errorConsumer.accept(message + ": " + e);
        }
    }

    /**
     * Shutdown all TCP connections and close the ServerSocket.
     * This method is idempotent. After returning, subsequent
     * attempts to call accept methods will fail until the open method is called.
     */
    @Override
    public synchronized void close()
    {
        LOGGER.debug("close");

        switch (_state) {
        case NO_SERVER_SOCKET:
            return;

        case ACCEPT_SINGLESHOT_REAPER_ACTIVE:
        case REAPER_ACTIVE:
        case ACCEPT_WITH_REUSE:
            try {
                stopSelectionLoop();
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while stopping selection loop");
            }
            break;
        }

        if (!_activeChannels.isEmpty()) {
            LOGGER.warn("Close called with active connections");
        }

        drainTo(_activeChannels, c -> closeChannel(c, "Failed to close still active channel"));
        drainTo(_idleChannels, c -> closeChannel(c, "Failed to close idle channel"));

        if (!_backgroundActiveChannels.isEmpty()) {
            LOGGER.warn("Close called with background active connections");
        }

        drainTo(_backgroundActiveChannels, bac -> closeChannel(bac.channel,
                "Failed to close background active channel"));

        synchronized (_selectorLock) {
            if (_channel != null) {
                closeChannel(_channel, "Failed to close listening socket");
                _channel = null;
            }
        }

        _state = State.NO_SERVER_SOCKET;
    }

    @Override
    public String toString()
    {
        int activeChannelCount;
        synchronized (_activeChannels) {
            activeChannelCount = _activeChannels.size();
        }

        int idleChannelCount;
        synchronized (_idleChannels) {
            idleChannelCount = _idleChannels.size();
        }

        ServerSocketChannel channel;
        Selector selector;
        synchronized (_selectorLock) {
            channel = _channel;
            selector = _selector;
        }

        return "ConnectionHandler[" + (channel != null ? "L" : "-" ) +
                (selector != null ? "S" : "-") +
                "; Conns (a:" + activeChannelCount +
                ", i:" + idleChannelCount +
                "); Counters (su-accept: " + _singleUseAccepts +
                ", reusable-accept: " + _reuseableAccepts +
                ", conn-reuse: " + _connectionReuse +
                ")]";
    }

    @GuardedBy("this")
    private void startConnectionReaper()
    {
        LOGGER.trace("starting connection reaper");
        assert _state != State.REAPER_ACTIVE;
        assert _selector == null;

        try {
            synchronized (_selectorLock) {
                _selector = Selector.open();
                registerAll(_idleChannels);
                _selectorFinishRequested = false;
            }
            Thread reaper = new Thread(this::reaper);
            String id = BaseEncoding.base64().omitPadding().encode(Ints.toByteArray(reaper.hashCode()));
            reaper.setName("ftp-reaper-" + id);
            reaper.setDaemon(true);
            reaper.start();
            _state = State.REAPER_ACTIVE;
        } catch (IOException e) {
            LOGGER.error("Failed to establish connection reaper: {}", e.toString());
        }
    }

    public void reaper()
    {
        try {
            selectionLoop(c -> _backgroundActiveChannels.add(new ActiveChannel(c, NO_DATA)),
                    channel -> {
                            ByteBuffer writeableBuffer = ensureCapacity(16, KiB);
                            try {
                                int count = channel.read(writeableBuffer);
                                if (count == -1) {
                                    LOGGER.debug("Closed channel detected");
                                    tryCloseChannel(channel, "Closing half-closed channel failed");
                                } else {
                                    ByteBuffer readableBuffer = detachForReading();
                                    LOGGER.debug("Idle channel now active, read {} bytes", readableBuffer.remaining());
                                    _backgroundActiveChannels.add(new ActiveChannel(channel, readableBuffer));
                                }
                            } catch (IOException e) {
                                LOGGER.error("Read in reaper on {} failed: {}", channel, e.toString());
                                tryCloseChannel(channel, "Defensive close after reaper read failure");
                            }
                        });
        } catch (IOException e) {
            LOGGER.error("Selector failed: {}", e.toString());
        }
    }

    private ByteBuffer ensureCapacity(int size, ByteUnit units)
    {
        int byteCount = units.toBytes(size);
        if (_data == null || _data.capacity() < byteCount) {
            _data = ByteBuffer.allocate(byteCount);
        }
        return _data;
    }

    private ByteBuffer detachForReading()
    {
        ByteBuffer data = (ByteBuffer) _data.flip();
        _data = null;
        return data;
    }

    /**
     * Close the channel.  Any fail is <emph>not</emph> reported to the error
     * consumer.
     * @param channel the channel to close
     * @param message the message describing the failure -- this should not end
     * with any punctuation.
     */
    private void tryCloseChannel(SocketChannel channel, String message)
    {
        try {
            channel.close();
        } catch (IOException ie) {
            LOGGER.error("{}: {}", message, ie.toString());
        }
    }

    private boolean removeIdleChannel(SocketChannel channel, String message)
    {
        if (!_idleChannels.remove(channel)) {
            LOGGER.warn("{}: {}", message, channel);
            return false;
        }
        return true;
    }
}
