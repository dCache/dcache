package org.dcache.ftp.data;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

/**
 * Multiplexer implements an event loop around a normal Java NIO
 * Selector and delegates each even to MultiplexerListener
 * implementations.
 *
 * Besides the infrastructure for registering listeners and the event
 * loop, this class provides little functionality.
 *
 * Notice that the multiplexer is not thread-safe.
 */
public class Multiplexer implements ErrorListener
{
    protected boolean _shutdown;
    protected Selector _selector;
    protected ErrorListener _errorListener;

    /**
     * Constructs a new multiplexer. The multiplexer must be destroyed
     * by a call to close().
     */
    public Multiplexer(ErrorListener errorListener) throws IOException {
        _shutdown      = false;
        _selector      = Selector.open();
        _errorListener = errorListener;
    }

    /** Log status messsages. */
    @Override
    public void say(String msg)
    {
        if (_errorListener != null) {
            _errorListener.say(msg);
        }
    }

    /** Log error messsages. */
    @Override
    public void esay(String msg)
    {
        if (_errorListener != null) {
            _errorListener.esay(msg);
        }
    }

    /** Log error messsages. */
    @Override
    public void esay(Throwable t)
    {
        if (_errorListener != null) {
            _errorListener.esay(t);
        }
    }

    /**
     * The event loop. The event loop continues running until
     * shutdown() is called or the current thread has been
     * interrupted.
     *
     * @throws InterruptedException
     */
    public void loop() throws Exception {
        while (!_shutdown) {
            _selector.select();

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            for (SelectionKey key : _selector.selectedKeys()) {
                MultiplexerListener listener =
                        (MultiplexerListener)key.attachment();
                if (key.isValid() && key.isConnectable() ) {
                    listener.connect(this, key);
                }
                if (key.isValid() && key.isAcceptable()) {
                    listener.accept(this, key);
                }
                if (key.isValid() && key.isReadable()) {
                    listener.read(this, key);
                }
                if (key.isValid() && key.isWritable()) {
                    listener.write(this, key);
                }
            }
            _selector.selectedKeys().clear();
        }
    }

    /**
     * Register a listener on the given channel. Only one listener can
     * be registered on any given channel. If a listener was already
     * registered, the old listener is silently unregistered and the
     * new listener is registered. The listener is registered for the
     * type of events specifified by the op bitmask (@see
     * SelectionKey).
     */
    public SelectionKey register(MultiplexerListener listener,
                                 int op, SelectableChannel channel)
            throws IOException
    {
        return channel.register(_selector, op, listener);
    }

    /**
     * Add a listener to the multiplexer. This is equivalent to
     * calling listener.register(multiplexer).
     */
    public void add(MultiplexerListener listener) throws Exception {
        listener.register(this);
    }

    /**
     * Closes the multiplexer. This closes the encapsulated selector
     * and all channels currently registered in the selector.
     */
    public void close() throws IOException {
        for (SelectionKey key : _selector.keys()) {
            key.channel().close();
        }
        _selector.selectNow();
        _selector.close();
    }

    /**
     * Shuts down the multiplexer, causing it to leave the event loop.
     */
    public void shutdown() {
        say("Multiplexer shutting down");
        _shutdown = true;
    }
}
