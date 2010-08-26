package org.dcache.pool.movers;

import java.nio.*;
import java.nio.channels.*;
import java.io.*;
import java.net.*;
import dmg.cells.nucleus.*;

public class NioSocketProvider 
{
    public static Socket getSocket(SocketAddress address) throws Exception 
    {
        SocketChannel socketChannel = SocketChannel.open(address);
        socketChannel.configureBlocking(true);
        return socketChannel.socket();
    }
}
