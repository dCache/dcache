package org.dcache.ftp.data;

import java.nio.channels.SelectionKey;

/** Empty implementation of MultiplexerListener. */
public class AbstractMultiplexerListener implements MultiplexerListener
{
    @Override
    public void register(Multiplexer multiplexer)
            throws Exception {}
    @Override
    public void accept(Multiplexer multiplexer, SelectionKey key)
            throws Exception {}
    @Override
    public void connect(Multiplexer multiplexer, SelectionKey key)
            throws Exception {}
    @Override
    public void read(Multiplexer multiplexer, SelectionKey key)
            throws Exception {}
    @Override
    public void write(Multiplexer multiplexer, SelectionKey key)
            throws Exception {}
}
