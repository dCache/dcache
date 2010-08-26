/*
 * $Id: NetRolesServerSocket.java,v 1.1 2004-10-06 08:08:27 tigran Exp $
 */

package javatunnel;

import java.net.*;
import java.io.*;

class NetRolesServerSocket extends ServerSocket {
    
    
    /** Creates a new instance of NetRolesServerSocket */
    public NetRolesServerSocket()  throws java.io.IOException {
        super();
    }
    
    public NetRolesServerSocket(int port)  throws java.io.IOException {
        super(port);
    }
    
    public NetRolesServerSocket(int port, int backlog)  throws java.io.IOException {
        super(port, backlog);
    }
    
    public NetRolesServerSocket(int port, int backlog, InetAddress bindAddr)  throws java.io.IOException {
        super(port, backlog, bindAddr);
    }
    
    public Socket accept() throws IOException {
        while (true) {
            
            if (isClosed())
                throw new SocketException("Socket is closed");
            if (!isBound())
                throw new SocketException("Socket is not bound yet");
            
            Socket s = new NetRolesSocket((SocketImpl) null);
            implAccept(s);
            
            ((NetRolesSocket)s).setUserPrincipal( s.getRemoteSocketAddress().toString() );
            
            return s;
        }
    }
    
}
