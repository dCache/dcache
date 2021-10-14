package org.dcache.ftp.data;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * Empty implementation of MultiplexerListener.
 */
public class AbstractMultiplexerListener implements MultiplexerListener {

    @Override
    public void register(Multiplexer multiplexer)
          throws IOException {
    }

    @Override
    public void accept(Multiplexer multiplexer, SelectionKey key)
          throws IOException {
    }

    @Override
    public void connect(Multiplexer multiplexer, SelectionKey key)
          throws IOException {
    }

    @Override
    public void read(Multiplexer multiplexer, SelectionKey key)
          throws IOException, FTPException, InterruptedException {
    }

    @Override
    public void write(Multiplexer multiplexer, SelectionKey key)
          throws IOException, FTPException {
    }
}
