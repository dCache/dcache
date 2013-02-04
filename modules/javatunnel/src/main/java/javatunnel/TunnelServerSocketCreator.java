/*
 * $Id: TunnelServerSocketCreator.java,v 1.1 2002-10-14 11:53:07 cvs Exp $
 */

package javatunnel;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.IOException;
import java.lang.reflect.*;

import javax.net.ServerSocketFactory;

public class TunnelServerSocketCreator extends ServerSocketFactory {


    Convertable _tunnel;

    public TunnelServerSocketCreator(String[] args)
            throws Throwable {

        super();

        Class<? extends Convertable> c  = Class.forName(args[0]).asSubclass(Convertable.class);
        Class<?> [] classArgs = { String.class } ;
        Constructor<? extends Convertable> cc = c.getConstructor(classArgs);
        Object[] a = new Object[1];
        a[0] = args[1];

        try {
            _tunnel = cc.newInstance(a);
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
