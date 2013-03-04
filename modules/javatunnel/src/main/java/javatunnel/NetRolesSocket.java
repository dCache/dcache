/*
 * $Id: NetRolesSocket.java,v 1.1 2004-10-06 08:08:27 tigran Exp $
 */

package javatunnel;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketImpl;
import java.net.UnknownHostException;

public class NetRolesSocket extends Socket {

    private String _user = "nobody@NOWHERE" ;

    NetRolesSocket()
    {
        super();
    }

    NetRolesSocket(SocketImpl impl) throws SocketException {
        super(impl);
    }

    NetRolesSocket(InetAddress address, int port)
    throws IOException {
        super(address, port);
    }


    NetRolesSocket(InetAddress address, int port, InetAddress localAddr, int localPort)
    throws IOException {
        super(address, port, localAddr, localPort);
    }

    NetRolesSocket(String host, int port)
    throws UnknownHostException, IOException {
        super(host, port);
    }

    NetRolesSocket(String host, int port, InetAddress localAddr, int localPort)
    throws IOException {
        super(host, port, localAddr, localPort);
    }

    public void setUserPrincipal(String user) {
        int i = user.indexOf(':');
        _user = user.substring(1,i);
    }


    public String getUserPrincipal() {
        return _user;
    }

}
