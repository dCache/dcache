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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.ProtocolFamily;
import java.net.Socket;
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
import java.util.function.Consumer;

import org.dcache.ftp.door.AbstractFtpDoorV1.Protocol;
import org.dcache.util.PortRange;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.find;

/**
 * Takes responsibility for opening a ServerSocket and handling
 * connections to that socket.  Provides an accept-like method that can reuse
 * existing TCP connections.
 */
public class PassiveConnectionHandler implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PassiveConnectionHandler.class);

    private final InetAddress _address;
    private final PortRange _portRange;
    private final List<SocketChannel> _activeChannels = new ArrayList<>();
    private final List<SocketChannel> _idleChannels = new ArrayList<>();
    private final List<SocketChannel> _toBeRegistered = new ArrayList<>();

    private Protocol _preferredProtocol;
    private Supplier<Iterable<InterfaceAddress>> _addressSupplier;
    private ServerSocketChannel _channel;
    private Consumer<String> _errorConsumer = (String s) -> {};
    private volatile Selector _selector;
    private volatile boolean _isAcceptFinished;
    private int _singleUseAccepts;
    private int _reuseableAccepts;
    private int _connectionReuse;

    public PassiveConnectionHandler(InetAddress address, PortRange range)
    {
        _address = address;
        _portRange = range;
    }

    public synchronized InetSocketAddress getLocalAddress()
    {
        return _channel == null ? null : (InetSocketAddress) _channel.socket().getLocalSocketAddress();
    }

    public synchronized void setAddressSupplier(Supplier<Iterable<InterfaceAddress>> addressSupplier)
    {
        checkState(_channel == null, "Cannot specify address supplier after socket opened.");
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

        if (_channel != null) {
            Protocol current = Protocol.fromAddress(_channel.socket().getInetAddress());
            if (preferred != current) {
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
     * multiple times: only the first call has an effect.  This call may not
     * be called after {@code #close} is called.
     */
    public synchronized void open() throws IOException
    {
        if (_channel == null) {
            InetAddress address = _address;
            if (_preferredProtocol != null && Protocol.fromAddress(address) != _preferredProtocol) {
                Iterable<InterfaceAddress> addresses = _addressSupplier.get();
                InterfaceAddress newAddress =
                        find(addresses, (a) -> Protocol.fromAddress(a.getAddress()).equals(_preferredProtocol));
                    address = newAddress.getAddress();
            }
            _channel = ServerSocketChannel.open();
            _portRange.bind(_channel.socket(), address);
            LOGGER.debug("Server socket opened {}", _channel);
        }
    }

    /**
     * Await for a TCP connection to be established.  The caller is
     * responsible for calling {@code Socket#close}.  This should not be called
     * at the same time as the other accept method.
     */
    public SocketChannel accept() throws IOException
    {
        ServerSocketChannel channel;
        synchronized (this) {
            checkState(_channel != null, "failed to call open");
            LOGGER.debug("Accepting output connection within ConnectionHandler on {}",
                 _channel.socket().getLocalSocketAddress());
            channel = _channel;
        }

        channel.configureBlocking(true);
        SocketChannel socket = channel.accept();

        socket.socket().setKeepAlive(true);

        synchronized (this) {
            _singleUseAccepts++;
        }

        return socket;
    }

    private synchronized boolean isAcceptFinish()
    {
        return _isAcceptFinished;
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
     * when presented with a channel.  The consumer may be called if an idle
     * connection is closed by the client, with the first read returning -1.
     * @param channelConsumer the object that will handle active channels.
     * @throws IOException if there is a problem while establishing connections.
     */
    public void accept(Consumer<SocketChannel> channelConsumer) throws IOException
    {
        synchronized (this) {
            checkState(_channel != null, "ServerSocket not opened");
            checkState(_selector == null, "accept already running");

            LOGGER.debug("Accepting input connection on {}", getLocalAddress());

            _selector = Selector.open();
            _isAcceptFinished = false;

            _channel.configureBlocking(false);
            _channel.register(_selector, SelectionKey.OP_ACCEPT);
            registerAll(_idleChannels);
        }

        List<SelectionKey> consumed = new ArrayList<>();
        while (!Thread.currentThread().isInterrupted() && !isAcceptFinish()) {
            _selector.select();

            for (SelectionKey key : _selector.selectedKeys()) {
                if (key.isAcceptable()) {
                    SocketChannel channel = _channel.accept();

                    synchronized (this) {
                        _activeChannels.add(channel);
                        _reuseableAccepts++;
                    }

                    LOGGER.debug("Opened {}", channel.socket().toString());
                    channel.socket().setKeepAlive(true);
                    channelConsumer.accept(channel);
                } else if (key.isReadable()) {
                    SocketChannel channel = (SocketChannel) key.channel();
                    LOGGER.debug("Idle channel became active: {}", channel);

                    synchronized (this) {
                        if (_idleChannels.remove(channel)) {
                            _activeChannels.add(channel);
                            key.interestOps(0);
                            _connectionReuse++;
                        } else {
                            LOGGER.warn("Received SelectionKey isReadable event for non-idle channel");
                            channel = null;
                        }
                    }

                    if (channel != null) {
                        channelConsumer.accept(channel);
                    }
                } else {
                    LOGGER.warn("Unknown SelectionKey event: {}", key.readyOps());
                }
                consumed.add(key);
            }
            _selector.selectedKeys().removeAll(consumed);
            consumed.clear();

            synchronized (this) {
                registerAll(_toBeRegistered);
                _idleChannels.addAll(_toBeRegistered);
                _toBeRegistered.clear();
            }
        }

        synchronized (this) {
            try {
                _selector.close();
            } finally {
                _selector = null;
                notifyAll();
            }
        }
    }

    private void registerAll(Collection<SocketChannel> channels)
    {
        Iterator<SocketChannel> itr = channels.iterator();
        while (itr.hasNext()) {
            if (!registerChannel(itr.next())) {
                itr.remove();
            }
        }
    }

    /**
     * Register a channel in the selector or close channel.  If a channel
     * is already registered then OP_READ selection is enabled.
     * @return true if channel is successfully registered.
     */
    private boolean registerChannel(SelectableChannel channel)
    {
        try {
            if (channel.isRegistered()) {
                channel.keyFor(_selector).interestOps(SelectionKey.OP_READ);
            } else {
                channel.configureBlocking(false).register(_selector,  SelectionKey.OP_READ);
            }
            return true;
        } catch (ClosedChannelException | ClosedSelectorException e) {
            LOGGER.debug("Failed to register channel in selector: {}", e.toString());
        } catch (IOException e) {
            LOGGER.warn("Failed to register channel in selector: {}", e.toString());
        }
        closeChannel(channel);
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
        LOGGER.trace("Channel returned: {}", channel);

        if (!_activeChannels.remove(channel)) {
            LOGGER.warn("returning channel that is not in active list.");
            closeChannel(channel);
            return;
        }

        Socket s = channel.socket();
        if (_channel == null || s.isInputShutdown() || s.isOutputShutdown()) {
            LOGGER.trace("Closing channel: {}", channel);
            closeChannel(channel);
        } else {
            LOGGER.trace("Registering idle channel: {}", channel);

            if (_selector == null) {
                _idleChannels.add(channel);
            } else {
                _toBeRegistered.add(channel);
                _selector.wakeup();
            }
        }
    }

    /**
     * Indicate that no further new connectors or
     * idle-connectors becoming active is expected.  If a thread called
     * {@code #accept(Consumer<SocketChannel>)} then it will return; if there
     * is no such thread then this method does nothing.  No further calls to
     * the {@literal Consumer<SocketChannel>} will be made once this method
     * returns.
     */
    public synchronized void finishAccept() throws InterruptedException
    {
        LOGGER.trace("finishAccept called");

        while (_selector != null) {
            _isAcceptFinished = true;
            _selector.wakeup();
            LOGGER.trace("Awaiting accept loop to terminate");
            wait();
        }
    }

    private void closeChannel(Channel channel)
    {
        try {
            channel.close();
        } catch (IOException e) {
            _errorConsumer.accept(e.getMessage());
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
        try {
            finishAccept();
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted while closing ConnectionHandler");
        }

        if (!_activeChannels.isEmpty()) {
            LOGGER.warn("close#ConnectionHandler called with active connections");
            _activeChannels.forEach(this::closeChannel);
            _activeChannels.clear();
        }

        _idleChannels.forEach(this::closeChannel);
        _idleChannels.clear();

        if (_channel != null) {
            LOGGER.debug("Closing passive mode server socket: {}", _channel);
            try {
                _channel.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close passive mode server socket: {}",
                        e.getMessage());
            } finally {
                _channel = null;
            }
        }
    }

    @Override
    public synchronized String toString()
    {
        return "ConnectionHandler[" + (_channel != null ? "L" : "-" ) +
                (_selector != null ? "S" : "-") +
                "; Conns (a:" + _activeChannels.size() +
                ", i:" + _idleChannels.size() +
                "); Counters (su-accept: " + _singleUseAccepts +
                ", reusable-accept: " + _reuseableAccepts +
                ", conn-reuse: " + _connectionReuse +
                ")]";
    }
}
