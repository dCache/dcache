/*
 * $Id: TunnelServerSocketCreator.java,v 1.1 2002-10-14 11:53:07 cvs Exp $
 */

package javatunnel;

import javax.net.ServerSocketFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.dcache.util.Args;

public class TunnelServerSocketCreator extends ServerSocketFactory {


    Convertable _tunnel;

    public TunnelServerSocketCreator(String arguments)
            throws Throwable
    {
        Args args = new Args(arguments);
        Class<? extends Convertable> c  = Class.forName(args.argv(0)).asSubclass(Convertable.class);
        args.shift();
        Constructor<? extends Convertable> cc = c.getConstructor(String.class);
        try {
            _tunnel = cc.newInstance(args.toString());
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }


    @Override
    public ServerSocket createServerSocket( int port ) throws IOException {
        return new TunnelServerSocket(port, _tunnel);
    }

    @Override
    public ServerSocket createServerSocket() throws IOException {
        return new TunnelServerSocket(_tunnel);
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog)
            throws IOException {
        return new TunnelServerSocket(port, backlog, _tunnel);
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog,
            InetAddress ifAddress) throws IOException {
        return new TunnelServerSocket(port, backlog, ifAddress, _tunnel);
    }
}
