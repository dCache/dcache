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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.dcache.cells.AbstractCell;

/**
 * Data channel proxy for FTP door. The proxy will run at the GridFTP
 * door and relay data between the client and the pool. Mode S and
 * mode E are supported. Mode E is only supported when data flows from
 * the client to the pool.
 *
 * The class is also used to establish data channels for transfering
 * directory listings. This use should be reconsidered, at it is
 * unrelated to the proxy functionality.
 */
public class SocketAdapter implements Runnable, ProxyAdapter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SocketAdapter.class);

    /** Channel listening for connections from the client. */
    private final ServerSocketChannel _clientListenerChannel;

    /** Channel listening for connections from the pool. */
    private final ServerSocketChannel _poolListenerChannel;

    /** Current number of data channel connections. */
    private int _dataChannelConnections;

    /** True if mode E is used for transfer, false when mode S is used. */
    private boolean _modeE;

    /**
     * Expected number of data channels. In mode S this is 1. In mode
     * E this information is provided by the client.
     */
    private int _eodc = 1;

    /**
     * The number of data channels we have closed. For informational
     * purposes only.
     */
    private int _dataChannelsClosed;

    /**
     * The number of EOD markers we have seen.
     */
    private int _eodSeen;

    /**
     * TCP send and receive buffer size. Will use default when zero.
     */
    private int _bufferSize;

    /**
     * True for uploads, false for downloads.
     */
    private boolean _clientToPool;

    /**
     * Non null if an error has occurred and the transfer has failed.
     */
    private String _error;

    /**
     * Selector for doing asynchronous accept.
     */
    private Selector _selector;

    /**
     * Size of the largest block allocated in mode E. Blocks larger
     * than this are divided into smaller blocks.
     */
    private int _maxBlockSize = 131072;

    /**
     * A thread driving the adapter
     */
    private Thread _thread;

    /**
     * True when the adapter is closing or has been closed. Used to
     * suppress error messages when killing the adapter.
     */
    private boolean _closing;

    /**
     * String form of address on which the adapter listens for client
     * connections.
     */
    private final String _localAddress;

    /**
     * A redirector moves data between an input channel and an ouput
     * channel. This particular redirector does so in mode S.
     */
    class StreamRedirector extends Thread
    {
	private SocketChannel _input, _output;

	public StreamRedirector(SocketChannel input, SocketChannel output)
	{
	    super("ModeS-Proxy-" + _localAddress);
	    _input = input;
	    _output = output;
	}

	@Override
        public void run()
	{
            String inputAddress =
                _input.socket().getRemoteSocketAddress().toString();
            String outputAddress =
                _output.socket().getRemoteSocketAddress().toString();
            boolean reading = true;
	    try {
		LOGGER.info("Starting mode S proxy from {} to {}",
                     inputAddress, outputAddress);
		ByteBuffer buffer = ByteBuffer.allocate(128 * 1024);
		while (_input.read(buffer) != -1) {
		    buffer.flip();
                    reading = false;
                    _output.write(buffer);
                    reading = true;
		    buffer.clear();
		}
		_input.close();
	    } catch (IOException e) {
                if (reading) {
                    setError("Error on socket to " + inputAddress + ": "
                            + e.getMessage());
                } else {
                    setError("Error on socket to " + outputAddress + ": "
                            + e.getMessage());
                }
	    } finally {
		subtractDataChannel();
	    }
	}
    }

    /**
     * A redirector moves data between an input channel and an ouput
     * channel. This particular redirector does so in mode E.
     */
    class ModeERedirector extends Thread
    {
	private SocketChannel _input, _output;

	public ModeERedirector(SocketChannel input, SocketChannel output)
	{
	    super("ModeE-Proxy-" + _localAddress);
	    _input  = input;
	    _output = output;
	}

	@Override
        public void run()
	{
	    boolean eod = false;
            boolean used = false;
            ByteBuffer header = ByteBuffer.allocate(17);
            EDataBlockNio block = new EDataBlockNio(getName());
            String inputAddress =
                _input.socket().getRemoteSocketAddress().toString();
            String outputAddress =
                _output.socket().getRemoteSocketAddress().toString();
            boolean reading = true;

            long count, position;

	    try {
		LOGGER.info("Starting mode E proxy from {} to {}",
                     inputAddress, outputAddress);

		loop: while (!eod && block.readHeader(_input) > -1) {
                    used = true;

                    /* EOF blocks are never forwarded as they do not
                     * contain any data and the SocketAdapter sends an
                     * EOF at the beginning of the stream. Other
                     * blocks are forwarded if they are not empty.
                     */
                    if (block.isDescriptorSet(EDataBlockNio.EOF_DESCRIPTOR)) {
                        setEODExpected(block.getDataChannelCount());
                        count = position = 0;
                    } else {
                        count = block.getSize();
                        position = block.getOffset();
                    }

                    /* Read and send a single block. To limit memory
                     * usage, we will read at most _maxBlockSize bytes
                     * at a time. Larger blocks are divided into
                     * multiple blocks.
                     */
                    while (count > 0) {
                        long len = Math.min(count, _maxBlockSize);
                        if (block.readData(_input, len) != len) {
                            break loop;
                        }

                        /* Generate output header.
                         */
                        header.clear();
                        header.put((byte)0);
                        header.putLong(len);
                        header.putLong(position);

                        /* Write output.
                         */
                        ByteBuffer[] buffers = {
                            header,
                            block.getData()
                        };
                        buffers[0].flip();
                        buffers[1].flip();
                        reading = false;
                        _output.write(buffers);
                        reading = true;

                        /* Update counters.
                         */
                        count = count - len;
                        position = position + len;
                    }

                    /* Check for EOD mark.
                     */
                    if (block.isDescriptorSet(EDataBlockNio.EOD_DESCRIPTOR)) {
                        eod = true;
                    }
                }

		if (eod) {
		    addEODSeen();
		} else if (used) {
		    setError("Data channel from " + inputAddress
                             + " was closed before EOD marker");
		}

		/* In case of an error, SocketAdapter will close the
		 * channel instead. We only call close here to free up
		 * sockets as early as possible when everything went
		 * as expected.
		 */
		_input.close();
	    } catch (Exception e) {
                if (reading) {
                    setError("Error on socket to " + inputAddress + ": "
                            + e.getMessage());
                } else {
                    setError("Error on socket to " + outputAddress + ": "
                            + e.getMessage());
                }
	    } finally {
		LOGGER.info("Redirector done, EOD = {}, used = {}", eod, used);
		subtractDataChannel();
	    }
	}
    }

    public SocketAdapter(ServerSocketChannel clientListenerChannel)
	throws IOException
    {
        _clientListenerChannel = clientListenerChannel;
        _poolListenerChannel = ServerSocketChannel.open();
	_poolListenerChannel.socket().bind(null);

        _localAddress =
            _clientListenerChannel.socket().getLocalSocketAddress().toString();
        _clientToPool = true;
        _modeE        = false;
        _eodSeen      = 0;
        _thread	      = new Thread(this, "SocketAdapter-" + _localAddress);
    }

    /** Increments the EOD seen counter. Thread safe. */
    protected synchronized void addEODSeen()
    {
        _eodSeen++;
    }

    /** Returns the EOD seen counter. Thread safe. */
    protected synchronized int getEODSeen()
    {
        return _eodSeen;
    }

    /** Returns the number of data channels to expect. Thread safe. */
    protected synchronized int getEODExpected()
    {
        return _eodc;
    }

    /**
     * Sets the number of data channels to expect. Thread safe.  The
     * selector will be woken up, since run() checks the data channel
     * count in the loop.
     */
    protected synchronized void setEODExpected(long count)
    {
	LOGGER.trace("Setting data channel count to {}", count);
	_selector.wakeup();
        _eodc = (int)count;
    }

    /** Called whenever a redirector finishes. Thread safe. */
    protected synchronized void subtractDataChannel()
    {
        _dataChannelConnections--;
        _dataChannelsClosed++;

        if (_eodc < Integer.MAX_VALUE) {
            LOGGER.trace("Closing redirector {}, remaining: {}, eodc says there will be: {}",
                    _dataChannelsClosed, _dataChannelConnections, getEODExpected());
        } else {
            LOGGER.trace("Closing redirector {}, remaining: {}",
                    _dataChannelsClosed, _dataChannelConnections);
        }
    }

    /** Called whenever a new redirector is created. Thread safe. */
    protected synchronized void addDataChannel()
    {
	_dataChannelConnections++;
    }

    /**
     * Returns the current number of concurrent data channel
     * connections. Thread safe.
     */
    protected synchronized int getDataChannelConnections()
    {
	return _dataChannelConnections;
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
                _thread.interrupt();
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
            LOGGER.error("Socket adapter {} caught fatal error: {}", _localAddress, e.getMessage());

            if (_error == null) {
                _thread.interrupt();
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

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#setModeE(boolean)
     */
    @Override
    public synchronized void setModeE(boolean modeE)
    {
        _modeE = modeE;
	// MAX_VALUE = unknown until EODC
        _eodc = (modeE ? Integer.MAX_VALUE : 1);
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#getClientListenerPort()
     */
    @Override
    public int getClientListenerPort()
    {
        return _clientListenerChannel.socket().getLocalPort();
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#getPoolListenerPort()
     */
    @Override
    public int getPoolListenerPort()
    {
        return _poolListenerChannel.socket().getLocalPort();
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#setDirClientToPool()
     */
    @Override
    public void setDirClientToPool()
    {
        _clientToPool = true;
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#setDirPoolToClient()
     */
    @Override
    public void setDirPoolToClient()
    {
        _clientToPool = false;
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
	assert _clientToPool || !_modeE;

        ServerSocketChannel inputSock;
        ServerSocketChannel outputSock;

        /** All redirectores created by the SocketAdapter. */
        List<Thread> redirectors = new ArrayList<>();

        /** All sockets created by the SocketAdapter. */
	List<SocketChannel> sockets     = new ArrayList<>();

        try {
	    _selector = Selector.open();

	    if (_clientToPool) {
		inputSock = _clientListenerChannel;
		outputSock = _poolListenerChannel;
	    } else {
		inputSock = _poolListenerChannel;
		outputSock = _clientListenerChannel;
	    }

	    /* Accept connection on output channel. Since the socket
	     * adapter is only used when the client is active, and
	     * since in mode E the active part has to be the sender,
	     * and since we only create one connection between the
	     * adapter and the pool, there will in any case be exactly
	     * one connection on the output channel.
	     */
	    LOGGER.debug("Accepting output connection on {}",
                 outputSock.socket().getLocalSocketAddress());
	    SocketChannel output = outputSock.accept();
	    sockets.add(output);
            if (_bufferSize > 0) {
                output.socket().setSendBufferSize(_bufferSize);
            }
            output.socket().setKeepAlive(true);
            LOGGER.debug("Opened {}", output.socket());

	    /* Send the EOF. The GridFTP protocol allows us to send
             * this information at any time. Doing it up front will
             * make sure, that the other end doesn't need to wait for
             * it.
	     */
	    if (_modeE) {
		ByteBuffer block = ByteBuffer.allocate(17);
		block.put((byte)(EDataBlockNio.EOF_DESCRIPTOR));
		block.putLong(0);
		block.putLong(1);
		block.flip();
		output.write(block);
	    }

	    /* Keep accepting connections on the input socket as long
	     * as we have not reached the number of streams the client
	     * told us we should expect.
	     *
	     * This loop is one of the few places in which we check
	     * the interrupted flag of the current thread: At most
	     * other places, blocking operations will throw an
	     * exception when the thread was interrupted, however
	     * select() will return normally.
	     */
	    LOGGER.debug("Accepting input connection on {}",
                 inputSock.socket().getLocalSocketAddress());
            int totalStreams = 0;
	    inputSock.configureBlocking(false);
	    inputSock.register(_selector, SelectionKey.OP_ACCEPT, null);
	    while (!Thread.currentThread().isInterrupted()
		   && totalStreams < getEODExpected()) {
		_selector.select();
                for (SelectionKey key : _selector.selectedKeys()) {
		    if (key.isAcceptable()) {
			SocketChannel input = inputSock.accept();
			sockets.add(input);
                        LOGGER.debug("Opened {}", input.socket());

			if (_bufferSize > 0) {
			    input.socket().setSendBufferSize(_bufferSize);
			}
                        input.socket().setKeepAlive(true);

			addDataChannel();

			Thread redir;
			if (_modeE) {
			    redir = new ModeERedirector(input, output);
			} else {
			    redir = new StreamRedirector(input, output);
			}
			redir.start();
			redirectors.add(redir);

			totalStreams++;
		    }
		}
		_selector.selectedKeys().clear();
	    }

	    /* Block until all redirector threads have terminated.
             */
	    LOGGER.trace("Waiting for all redirectors to finish");
            for (Thread redirector : redirectors) {
		redirector.join();
	    }
	    redirectors.clear();
	    LOGGER.trace("All redirectors have finished");

	    /* Send the EOD (remember that we already sent the EOF
             * earlier).
	     */
	    if (_modeE) {
                if (getEODExpected() == Integer.MAX_VALUE) {
                    setError("Did not receive EOF marker. Transfer failed.");
                } else if (getEODSeen() != getEODExpected()) {
		    setError("Did not see enough EOD markers. Transfer failed.");
		} else {
                    ByteBuffer block = ByteBuffer.allocate(17);
                    block.put((byte)EDataBlockNio.EOD_DESCRIPTOR);
                    block.putLong(0);
                    block.putLong(0);
                    block.flip();
                    output.write(block);
                }
	    }
        } catch (InterruptedException e) {
            /* This will always be a symptom of another error, so
             * there is no reason to log this exception.
             */
        } catch (IOException e) {
            setError(e.getMessage());
        } catch (Exception e) {
	    setFatalError(e);
        } finally {
	    /* Close the selector. Any keys are automatically cancelled.
	     */
	    if (_selector != null) {
		try {
		    _selector.close();
		    _selector = null;
		} catch (IOException e) {
		    setError(e.getMessage());
		}
	    }

	    /* Tell any thread still alive to stop. In principal this
	     * should not be necessary since closing the channels
	     * below should cause all redirectors to break out. On the
	     * other hand it doesn't hurt and better safe than
	     * sorry...
	     */
            for (Thread redirector : redirectors) {
		redirector.interrupt();
	    }

	    /* Close all channels. The redirectors may already have
	     * closed the channels, however close() is a noop if the
	     * channel is already closed.
	     */
            for (SocketChannel channel : sockets) {
		try {
		    channel.close();
		} catch (IOException e) {
		    setError(e.getMessage());
		}
	    }
        }
    }

    /* (non-Javadoc)
     * @see diskCacheV111.util.ProxyAdapter#close()
     */
    @Override
    public void close() {
	LOGGER.debug("Closing listener sockets");

        setClosing(true);

	/* Interrupting this thread is enough to cause
	 * SocketAdapter.run() to break. SocketAdapter.run() will in
	 * turn interrupt all redirectors.
	 */
	_thread.interrupt();

        try {
            _poolListenerChannel.close();
	} catch (IOException e) {
            LOGGER.warn("Failed to close pool socket: {}", e.getMessage());
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
     * @see diskCacheV111.util.ProxyAdapter#join()
     */
    @Override
    public void join() throws InterruptedException {
	_thread.join();
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
	_thread.start();
    }

    @Override
    public String toString()
    {
        return "passiv; " + _dataChannelConnections + " streams created";
    }
}
