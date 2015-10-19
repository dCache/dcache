/*
 * $Id: NetRolesServerSocket.java,v 1.1 2004-10-06 08:08:27 tigran Exp $
 */

package javatunnel;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

class NetRolesServerSocket extends ServerSocket {


    /** Creates a new instance of NetRolesServerSocket */
    public NetRolesServerSocket()  throws IOException {
        super();
    }

    public NetRolesServerSocket(int port)  throws IOException {
        super(port);
    }

    public NetRolesServerSocket(int port, int backlog)  throws IOException {
        super(port, backlog);
    }

    public NetRolesServerSocket(int port, int backlog, InetAddress bindAddr)  throws IOException {
        super(port, backlog, bindAddr);
    }

    @Override
    public Socket accept() throws IOException {
        while (true) {

            if (isClosed()) {
                throw new SocketException("Socket is closed");
            }
            if (!isBound()) {
                throw new SocketException("Socket is not bound yet");
            }

            Socket s = new NetRolesSocket(null);
            implAccept(s);

            ((NetRolesSocket)s).setUserPrincipal( s.getRemoteSocketAddress().toString() );

            return s;
        }
    }

}
