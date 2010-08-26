package org.dcache.ftp;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/** Empty implementation of MultiplexerListener. */
public class AbstractMultiplexerListener implements MultiplexerListener
{
    public void register(Multiplexer multiplexer) 
	throws Exception {}
    public void accept(Multiplexer multiplexer, SelectionKey key) 
	throws Exception {}
    public void connect(Multiplexer multiplexer, SelectionKey key) 
	throws Exception {}
    public void read(Multiplexer multiplexer, SelectionKey key) 
	throws Exception {}
    public void write(Multiplexer multiplexer, SelectionKey key) 
	throws Exception {}
}
