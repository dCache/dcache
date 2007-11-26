package org.dcache.ftp;

import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.ClosedChannelException;
import java.io.IOException;

/** Implementation of MODE S. */
public class ModeS extends Mode
{
    public static final int BLOCK_SIZE = 512 * 1024;

    /* Implements MODE S send operation. */
    private class Sender extends AbstractMultiplexerListener
    {
	protected SocketChannel _socket;
	protected long _position;
	protected long _count;
	
	public Sender(SocketChannel socket) 
	{
	    _socket   = socket;
	    _position = getStartPosition();
	    _count    = getSize();
	}

	public void register(Multiplexer multiplexer) throws IOException 
	{
	    multiplexer.register(this, SelectionKey.OP_WRITE, _socket);
	}

	public void write(Multiplexer multiplexer, SelectionKey key) 
	    throws Exception 
	{
	    long nbytes = transferTo(_position, _count, _socket);
	    _monitor.sentBlock(_position, nbytes);

	    _position += nbytes;
	    _count    -= nbytes;

	    /* There is no special end-of-file signal in mode S. Just
	     * close the connection.
	     */
	    if (_count == 0) {
		close(multiplexer, key, true);
	    }
	}
    }

    /* Implements MODE S receive operation. */
    private class Receiver extends AbstractMultiplexerListener
    {
	protected SocketChannel _socket;
	protected long          _position;
	
	public Receiver(SocketChannel socket) 
	{
	    _socket   = socket;
	    _position = 0;
	}

	public void register(Multiplexer multiplexer) throws IOException 
	{
	    multiplexer.register(this, SelectionKey.OP_READ, _socket);
	}
	
	public void read(Multiplexer multiplexer, SelectionKey key) 
	    throws Exception
	{
	    _monitor.preallocate(_position + BLOCK_SIZE);
	    long nbytes = transferFrom(_socket, _position, BLOCK_SIZE);
	    if (nbytes == -1) {
		close(multiplexer, key, true);
	    } else {
		_monitor.receivedBlock(_position, nbytes);
		_position += nbytes;
	    }
	}
    }
    
    public ModeS(Role role, FileChannel file, ConnectionMonitor monitor)
	throws IOException 
    {
	super(role, file, monitor);
    }

    public void newConnection(Multiplexer multiplexer, SocketChannel socket) 
	throws Exception
    {
	switch (_role) {
	case Sender:
	    multiplexer.add(new Sender(socket));
	    break;
	case Receiver:
	    multiplexer.add(new Receiver(socket));
	    break;
	}
    }    
}
