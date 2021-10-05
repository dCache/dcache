package org.dcache.ftp.data;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * Interface of multiplexer listeners. Implementations can be registered with the multiplexer.
 */
public interface MultiplexerListener {

    /**
     * Called by the multiplexer upon adding the listener.
     */
    void register(Multiplexer multiplexer) throws IOException;

    /**
     * Called upon the channel being acceptable.
     */
    void accept(Multiplexer multiplexer, SelectionKey key) throws IOException;

    /**
     * Called upon the channel being connectable.
     */
    void connect(Multiplexer multiplexer, SelectionKey key) throws IOException;

    /**
     * Called upon the channel being readable.
     */
    void read(Multiplexer multiplexer, SelectionKey key)
          throws IOException, FTPException, InterruptedException;

    /**
     * Called upon the channel being writable.
     */
    void write(Multiplexer multiplexer, SelectionKey key) throws IOException, FTPException;
}
