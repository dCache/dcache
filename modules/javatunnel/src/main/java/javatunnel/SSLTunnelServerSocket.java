/*
 * $Id: SSLTunnelServerSocket.java,v 1.5 2002-10-22 12:44:43 cvs Exp $
 */

package javatunnel;


import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLServerSocketFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

public class SSLTunnelServerSocket extends ServerSocket {


	private final ServerSocket sock ;
	private final UserValidatable uv ;

	public SSLTunnelServerSocket(int port, ServerSocketFactory ssf, UserValidatable v) throws IOException {

		sock = ssf.createServerSocket( port );
		uv = v;

	}


    public SSLTunnelServerSocket( ServerSocketFactory ssf, UserValidatable v) throws IOException {

        sock = ssf.createServerSocket();
        uv = v;
    }

	public SSLTunnelServerSocket(int port, int backlog, InetAddress ifAddress, SSLServerSocketFactory ssf, UserValidatable v) throws IOException {
        sock = ssf.createServerSocket(port, backlog, ifAddress);
        uv = v;
    }


    public SSLTunnelServerSocket(int port, int backlog, SSLServerSocketFactory ssf, UserValidatable v) throws IOException {
        sock = ssf.createServerSocket(port, backlog);
        uv = v;
    }


    @Override
    public Socket accept() throws IOException {
		Socket s = new SSLTunnelSocket( sock.accept(), uv );
		return s;
	}


	@Override
	public void bind(SocketAddress endpoint) throws IOException {
	    sock.bind(endpoint);
	}

}
