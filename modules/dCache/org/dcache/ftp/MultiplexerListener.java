package org.dcache.ftp;

import java.nio.channels.SelectionKey;
import java.io.IOException;

/**
 * Interface of multiplexer listeners. Implementations can be
 * registered with the multiplexer.
 */
public interface MultiplexerListener
{
    /** Called by the multiplexer upon adding the listener. */
    void register(Multiplexer multiplexer) throws Exception;

    /** Called upon the channel being acceptable. */
    void accept(Multiplexer multiplexer, SelectionKey key) throws Exception;

    /** Called upon the channel being connectable. */
    void connect(Multiplexer multiplexer, SelectionKey key) throws Exception;

    /** Called upon the channel being readable. */
    void read(Multiplexer multiplexer, SelectionKey key) throws Exception;

    /** Called upon the channel being writable. */
    void write(Multiplexer multiplexer, SelectionKey key) throws Exception;
}
