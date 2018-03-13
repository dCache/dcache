/*
  COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


  Distribution of the software available from this server is free of
  charge subject to the user following the terms of the Fermitools
  Software Legal Information.

  Redistribution and/or modification of the software shall be accompanied
  by the Fermitools Software Legal Information  (including the copyright
  notice).

  The user is asked to feed back problems, benefits, and/or suggestions
  about the software to the Fermilab Software Providers.


  Neither the name of Fermilab, the  URA, nor the names of the contributors
  may be used to endorse or promote products derived from this software
  without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
*/

package org.dcache.ftp.proxy;

import com.google.common.io.BaseEncoding;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Ints;
import org.dcache.ftp.TransferMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import org.dcache.util.NDC;
import org.dcache.util.PortRange;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.ftp.TransferMode.MODE_E;
import static org.dcache.ftp.TransferMode.MODE_S;
import static org.dcache.ftp.proxy.ProxyAdapter.Direction.UPLOAD;
import static org.dcache.util.ByteUnit.KiB;

/**
 * The SocketAdapter relays data by listening on two server sockets.  The
 * behaviour is perhaps easiest to understand consider two roles: the data
 * sender and data recipient.  For a upload transfer, the data sender is the
 * client and the data receiver is the pool.  For a download transfer, these
 * roles are reversed.
 * <p>
 * Independent of whether the transfer is an upload or download, or whether
 * the data protocol is MODE_S or MODE_E, there is only a single TCP connection
 * from the door to the data recipient.  Once this single data recipient
 * connection is established, the adapter accepts (potentially multiple)
 * incoming data sender connections.
 * <p>
 * If the client connection is using MODE_S then the data sender establishes
 * only a single TCP connection.  The connection to the pool also uses MODE_S.
 * Data is simply read from the data sender and written to the data receiver.
 * <p>
 * If the client connection is using MODE_E then the data sender must be client
 * (i.e., the transfer is an upload).  The connection to the pool also uses
 * MODE_E.  The client may open multiple TCP connections; therefore, care is
 * taken to ensure complete blocks are sent when multiplexing the single pool
 * connection.
 */
public class SocketAdapter implements Runnable, ProxyAdapter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SocketAdapter.class);

    /** Connection orientated ChannelHandler references. */
    private final PassiveConnectionHandler _clientConnectionHandler;
    private final PassiveConnectionHandler _poolConnectionHandler;

    /** Data direction orientated ChannelHandler references. */
    private PassiveConnectionHandler _inbound;
    private PassiveConnectionHandler _outbound;

    private TransferMode _mode;

    /**
     * The number of EOD markers we have seen. This is only used for mode E
     * transfers.
     */
    private int _eodSeen;

    /**
     * The number of EOD markers expected, or zero if that number is
     * not yet known.  This is only used for mode E transfers.
     */
    private long _eodExpected;

    private Direction _direction = Direction.UPLOAD;

    /**
     * Non null if an error has occurred and the transfer has failed.
     */
    private String _error;

    /** All redirectores created by the SocketAdapter. */
    private final List<Redirector> _redirectors = new ArrayList<>();

    /**
     * Size of the largest block allocated in mode E. Blocks larger
     * than this are divided into smaller blocks.
     */
    private int _maxBlockSize = KiB.toBytes(128);

    /**
     * A thread driving the adapter
     */
    private final Thread _thread;

    /**
     * True when the adapter is closing or has been closed. Used to
     * suppress error messages when killing the adapter.
     */
    private boolean _closing;

    private SocketChannel _output;
    private String _outputLocalAddress = "awaiting";
    private String _outputRemoteAddress = "awaiting";


    private abstract class Redirector extends Thread
    {
        protected final SocketChannel _input;
        protected final String _inputLocalAddress;
        protected final String _inputRemoteAddress;
        private final String _shortName;

        Redirector(SocketChannel input, String namePrefix)
        {
            _input = requireNonNull(input);

            _shortName = namePrefix + "-proxy-"
                    + BaseEncoding.base64().omitPadding().encode(Ints.toByteArray(hashCode()));
            setName(_shortName + "-" + SocketAdapter.toString(_clientConnectionHandler.getLocalAddress()));

            _inputRemoteAddress = SocketAdapter.toString(_input.socket().getRemoteSocketAddress());
            _inputLocalAddress = SocketAdapter.toString(_input.socket().getLocalSocketAddress());
        }

        public abstract void runProxy();

        @Override
        public void run()
        {
            try {
                NDC.push(_shortName);
                LOGGER.debug("Accepting data: {} --> {}", _inputRemoteAddress,
                        _inputLocalAddress);
                LOGGER.debug("Sending data: {} --> {}", _outputLocalAddress,
                        _outputRemoteAddress);
                runProxy();
            } finally {
                NDC.pop();
            }
        }
    }

    /**
     * A redirector moves data between an input channel and an output
     * channel. This particular redirector does so in mode S.
     */
    class StreamRedirector extends Redirector
    {
        private final ByteBuffer _initial;
        public StreamRedirector(SocketChannel input, ByteBuffer initial)
        {
            super(input, "ModeS");
            _initial = initial;
        }

        @Override
        public void runProxy()
        {
            try {
                LOGGER.debug("Initial data: {}", _initial);
                _inbound.finishAccept(); // only expect a single connection.

                boolean reading = false;
                try {
                    _output.write(_initial);
                    reading = true;
                    ByteBuffer buffer = ByteBuffer.allocate(KiB.toBytes(128));
                    while (_input.read(buffer) != -1) {
                        buffer.flip();
                        reading = false;
                        _output.write(buffer);
                        reading = true;
                        buffer.clear();
                    }
                    markInputClosed(_input);
                } catch (IOException e) {
                    if (reading) {
                        setError("Error reading from " + _inputRemoteAddress + ": "
                                         + e.getMessage());
                    } else {
                        setError("Error writing to " + _outputRemoteAddress + ": "
                                         + e.getMessage());
                    }
                } finally {
                    LOGGER.debug("Returning input channel");
                    _inbound.returnChannel(_input);
                }
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while waiting for accept loop to terminate");
            }
        }

        private void markInputClosed(SocketChannel channel)
        {
            try {
                channel.shutdownInput();
            } catch (IOException e) {
                LOGGER.error("Failed to mark socket {} closed: {}", channel, e.toString());
            }
        }
    }

    /**
     * A redirector moves data between an input channel and an output
     * channel. This particular redirector does so in mode E.
     */
    class ModeERedirector extends Redirector
    {
        private final ByteBuffer _initial;

        public ModeERedirector(SocketChannel input, ByteBuffer initial)
        {
            super(input, "ModeE");
            _initial = initial;
        }

        @Override
        public void runProxy()
        {
            boolean eod = false;
            boolean used = false;
            try {
                LOGGER.debug("Initial data: {}", _initial);
                ByteBuffer header = ByteBuffer.allocate(17);
                EDataBlockNio block = new EDataBlockNio(getName(), _initial);
                boolean reading = true;

                long count, position;

                try {
                    loop: while (!eod && block.readHeader(_input) > -1) {
                        used = true;
                        /* EOF blocks are never forwarded as they do not
                         * contain any data and the SocketAdapter sends an
                         * EOF at the beginning of the stream. Other
                         * blocks are forwarded if they are not empty.
                         */
                        if (block.isDescriptorSet(EDataBlockNio.EOF_DESCRIPTOR)) {
                            long conns = block.getDataChannelCount();
                            if (conns <= 0) {
                                throw new IOException("Invalid Data Channel Count value: " + conns);
                            }
                            setEODExcepted(conns);
                            if (hasSeenAllExpectedEOD()) {
                                LOGGER.debug("Finishing accept");
                                _inbound.finishAccept();
                            }
                            count = position = 0;
                            LOGGER.debug("EOF descriptor: conns={}", conns);
                        } else {
                            count = block.getSize();
                            position = block.getOffset();
                            LOGGER.debug("Read {} bytes for offset {}", count, position);
                        }

                        /* Read and send a single block. To limit memory
                         * usage, we will read at most _maxBlockSize bytes
                         * at a time. Larger blocks are divided into
                         * multiple blocks.
                         */
                        while (count > 0) {
                            long len = Math.min(count, _maxBlockSize);
                            long actual = block.readData(_input, len);
                            if (actual != len) {
                                break loop;
                            }

                            /* Generate output header. */
                            header.clear();
                            header.put((byte)0);
                            header.putLong(len);
                            header.putLong(position);

                            /* Write output. */
                            ByteBuffer[] buffers = {
                                    header,
                                    block.getData()
                            };
                            buffers[0].flip();
                            buffers[1].flip();
                            reading = false;
                            LOGGER.debug("Sending {} bytes", len);
                            _output.write(buffers);
                            reading = true;

                            /* Update counters. */
                            count = count - len;
                            position = position + len;
                        }

                        /* Check for EOD mark. */
                        if (block.isDescriptorSet(EDataBlockNio.EOD_DESCRIPTOR)) {
                            LOGGER.debug("Block just received contains EOD");
                            eod = true;
                        }
                    }

                    if (eod) {
                        incrementEODSeen();
                        if (hasSeenAllExpectedEOD()) {
                            LOGGER.debug("Finishing accept");
                            _inbound.finishAccept();
                        }
                    } else if (used) {
                        setError("Data channel from " + _inputRemoteAddress
                                + " was closed before EOD marker");
                    }
                } catch (InterruptedException e) {
                    setError("Interrupted waiting for accept to shut down");
                } catch (IOException e) {
                    if (reading) {
                        setError("Error reading from " + _inputRemoteAddress + ": "
                                         + e.getMessage());
                    } else {
                        setError("Error writing to " + _outputRemoteAddress + ": "
                                         + e.getMessage());
                    }
                }
            } finally {
                LOGGER.debug("Returning input channel");
                _inbound.returnChannel(_input);
            }
        }
    }

    /**
     * Provide a better string representation than String.valueOf(..).  The
     * default representation always includes a '/' to seperate the hostname
     * from the IP address, even if the hostname is not supplied.  In addition,
     * any IPv6 address is not compressed.
     */
    private static String toString(SocketAddress sockAddr)
    {
        if (sockAddr == null) {
            return "disconnected";
        } else if (sockAddr instanceof InetSocketAddress) {
            InetSocketAddress inetSockAddr = (InetSocketAddress) sockAddr;
            InetAddress ip = inetSockAddr.getAddress();
            String host = ip == null ? inetSockAddr.getHostString() : InetAddresses.toAddrString(ip);
            return host + ":" + inetSockAddr.getPort();
        } else {
            return sockAddr.toString();
        }
    }


    public SocketAdapter(PassiveConnectionHandler handler, InetAddress addressForPools)
            throws IOException
    {
        _clientConnectionHandler = handler;
        _clientConnectionHandler.setErrorConsumer(this::setError);

        _poolConnectionHandler = new PassiveConnectionHandler(addressForPools, PortRange.ANY);
        _poolConnectionHandler.open();

        _thread = new Thread(this, "SocketAdapter-" + toString(_clientConnectionHandler.getLocalAddress()));
    }

    /** Increments the EOD seen counter. Thread safe. */
    private synchronized void incrementEODSeen()
    {
        _eodSeen++;
    }

    /** Returns the EOD seen counter. Thread safe. */
    private synchronized int getEODSeen()
    {
        return _eodSeen;
    }

    /**
     * Specify expected number of streams, so therefore expected number
     * of EOD.
     */
    private synchronized void setEODExcepted(long value)
    {
        checkArgument(value > 0);
        _eodExpected = value;
    }

    /**
     * Returns true if the expected number of EOD is known and the
     * number of observed EOD matches.
     */
    private synchronized boolean hasSeenAllExpectedEOD()
    {
        return isEODExpectedSpecified() && _eodSeen == _eodExpected;
    }

    /**
     * @return true iff setEODExpected has been called.
     */
    private synchronized boolean isEODExpectedSpecified()
    {
        return _eodExpected > 0;
    }

    /** Returns the EOD seen counter. Thread safe. */
    private synchronized long getEODExpected()
    {
        return _eodExpected;
    }

    /**
     * Sets the error field. This indicates that the transfer has
     * failed.
     */
    protected synchronized void setError(String msg)
    {
        if (!isClosing()) {
            LOGGER.error(msg);
            if (_error == null) {
                _inbound.close();
                _error = msg;
            }
        }
    }

    /**
     * Sets the error field. This indicates that the transfer has
     * failed. The SocketAdapter thread is interrupted, causing it to
     * break out of the run() method.
     */
    protected synchronized void setFatalError(Exception e)
    {
        if (!isClosing()) {
            LOGGER.error("Socket adapter {} caught fatal error: {}",
                    _clientConnectionHandler.getLocalAddress(), e.getMessage());

            if (_error == null) {
                _inbound.close();
                _error = e.getMessage();
            }
        }
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#getError()
     */
    @Override
    public synchronized String getError()
    {
        return _error;
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#hasError()
     */
    @Override
    public synchronized boolean hasError()
    {
        return _error != null;
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#setMaxBlockSize(int)
     */
    @Override
    public void setMaxBlockSize(int size)
    {
        _maxBlockSize = size;
    }

    @Override
    public synchronized void setMode(TransferMode mode)
    {
        checkArgument(mode == MODE_S || mode == MODE_E, "Unsupported transfer mode %s", mode);
        _mode = mode;
    }

    @Override
    public InetSocketAddress getInternalAddress()
    {
        return _poolConnectionHandler.getLocalAddress();
    }

    @Override
    public void setDataDirection(Direction dir)
    {
        _direction = dir;

        switch (dir) {
        case UPLOAD:
            _inbound = _clientConnectionHandler;
            _outbound = _poolConnectionHandler;
            break;
        case DOWNLOAD:
            _inbound = _poolConnectionHandler;
            _outbound = _clientConnectionHandler;
            break;
        }
    }

    /**
     * Sets the closing flag. @see _closing
     */
    private synchronized void setClosing(boolean closing)
    {
        _closing = closing;
    }

    /**
     * Returns the value of the closing flag. @see _closing
     */
    private synchronized boolean isClosing()
    {
        return _closing;
    }

    @Override
    public void run()
    {
        LOGGER.debug("Socket adapter thread starting");
        assert _direction == UPLOAD || _mode == MODE_S;

        try {
            /* Accept connection on output channel. Since the socket
             * adapter is only used when the client is active, and
             * since in mode E the active part has to be the sender,
             * and since we only create one connection between the
             * adapter and the pool, there will in any case be exactly
             * one connection on the output channel.
             */
            _output = _outbound.accept();
            _outputRemoteAddress = SocketAdapter.toString(_output.socket().getRemoteSocketAddress());
            _outputLocalAddress = SocketAdapter.toString(_output.socket().getLocalSocketAddress());


            try {
                /* Send the EOF. The GridFTP protocol allows us to send
                 * this information at any time. Doing it up front will
                 * make sure, that the other end doesn't need to wait for
                 * it.
                 */
                if (_mode == MODE_E) {
                    sendEof();
                }

                _inbound.accept(this::acceptNewChannel);

                awaitRedirectors();

                /* Send the EOD (remember that we already sent the EOF
                 * earlier).
                 */
                if (_mode == MODE_E) {
                    if (!isEODExpectedSpecified()) {
                        setError("Did not receive EOF marker. Transfer failed.");
                    } else if (!hasSeenAllExpectedEOD()) {
                        setError("Transfer failed: not enough EOD markers (expected " +
                                getEODExpected() + ", got " + getEODSeen() + ")");
                    } else {
                        sendEod();
                    }
                }
            } finally {
                try {
                    _output.close();
                    _outputLocalAddress = "closed";
                    _outputRemoteAddress = "closed";
                } catch (IOException e) {
                    LOGGER.warn("Problem closing output: {}", e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            /* This will always be a symptom of another error, so
             * there is no reason to log this exception.
             */
        } catch (IOException e) {
            setError(e.getMessage());
        } catch (RuntimeException e) {
            _thread.getUncaughtExceptionHandler().uncaughtException(_thread, e);
            setFatalError(e);
        } finally {
            /* Tell any thread still alive to stop. In principal this
             * should not be necessary since closing the channels
             * below should cause all redirectors to break out. On the
             * other hand it doesn't hurt and better safe than
             * sorry...
             */
            for (Thread redirector : _redirectors) {
                redirector.interrupt();
            }

            /* Close down everything on the pool side. */
            _poolConnectionHandler.close();
        }
    }

    private void sendEof() throws IOException
    {
        ByteBuffer block = ByteBuffer.allocate(17);
        block.put((byte)(EDataBlockNio.EOF_DESCRIPTOR));
        block.putLong(0);
        block.putLong(1);
        block.flip();
        _output.write(block);
    }

    private void sendEod() throws IOException
    {
        ByteBuffer block = ByteBuffer.allocate(17);
        block.put((byte)EDataBlockNio.EOD_DESCRIPTOR);
        block.putLong(0);
        block.putLong(0);
        block.flip();
        _output.write(block);
    }

    private void awaitRedirectors() throws InterruptedException
    {
        LOGGER.debug("Waiting for {} redirectors to finish", _redirectors.size());
        for (Thread redirector : _redirectors) {
            redirector.join();
        }
        _redirectors.clear();

        LOGGER.debug("All redirectors have finished");
    }

    private void acceptNewChannel(SocketChannel input, ByteBuffer initialInput)
    {
        LOGGER.debug("Accepting new TCP connection");

        Redirector redir;
        switch (_mode) {
        case MODE_E:
            redir = new ModeERedirector(input, initialInput);
            break;
        case MODE_S:
            redir = new StreamRedirector(input, initialInput);
            break;
        default:
            throw new RuntimeException("Unsupported transfer mode: " + _mode);
        }
        redir.start();

        _redirectors.add(redir);
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#close()
     */
    @Override
    public void close() {
        LOGGER.debug("Closing listener sockets");
        _poolConnectionHandler.close();

        setClosing(true);
        try {
            _inbound.finishAccept();
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted while closing SocketAdapter");
        }
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#isAlive()
     */
    @Override
    public boolean isAlive() {
        return _thread.isAlive();
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#join(long)
     */
    @Override
    public void join(long millis) throws InterruptedException {
        _thread.join(millis);
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#start()
     */
    @Override
    public void start() {
        LOGGER.debug("SocketAdapter calling _thread.start");
        _thread.start();
    }

    @Override
    public String toString() {
        return "SocketAdapter[mode=" + _mode.getLabel() + " in=" + _inbound + ", out=" + _outbound + "]";
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Passive adapter:");
        pw.println("    Transfer mode: " + _mode.getLabel());
        pw.println("    Listening on:");
        pw.println("        Client: " + _clientConnectionHandler.getLocalAddress());
        pw.println("        Pool: " + _poolConnectionHandler.getLocalAddress());
        pw.println("    Proxy status:");
        ProxyPrinter proxy = new ProxyPrinter();
        Socket out = _output.socket();
        boolean isFirstRow = true;
        for (Redirector redirector : _redirectors) {
            if (isFirstRow) {
                if (_direction == UPLOAD) {
                    proxy.pool(out);
                } else {
                    proxy.client(out);
                }
                isFirstRow = false;
            }
            Socket in = redirector._input.socket();
            if (_direction == UPLOAD) {
                proxy.client(in);
            } else {
                proxy.pool(in);
            }
            proxy.add();
        }
        pw.println(proxy);
    }
}
