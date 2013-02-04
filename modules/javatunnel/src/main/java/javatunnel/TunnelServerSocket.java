/*
 * $Id: TunnelServerSocket.java,v 1.6 2007-04-05 20:07:39 podstvkv Exp $
 */

package javatunnel;

import java.net.*;
import java.io.*;

class TunnelServerSocket extends ServerSocket {

    private Convertable _tunnel;
//  private UserBindible _userBind = null;

    /** Creates a new instance of TunnelServerSocket */
    public TunnelServerSocket( Convertable tunnel)  throws IOException {
        super();
        _tunnel = tunnel;
    }

    public TunnelServerSocket(int port, Convertable tunnel)  throws IOException {
        super(port);
        _tunnel = tunnel;
    }

    public TunnelServerSocket(int port, int backlog, Convertable tunnel)  throws IOException {
        super(port, backlog);
        _tunnel = tunnel;
    }

    public TunnelServerSocket(int port, int backlog, InetAddress bindAddr, Convertable tunnel)  throws IOException {
        super(port, backlog, bindAddr);
        _tunnel = tunnel;
    }

    @Override
    public Socket accept() throws IOException {
//      while (true) {
            Convertable tunnelCopy = _tunnel.makeCopy();
//          _userBind = (UserBindible)tunnelCopy;

            if (isClosed()) {
                throw new SocketException("Socket is closed");
            }
            if (!isBound()) {
                throw new SocketException("Socket is not bound yet");
            }

            Socket s = new TunnelSocket(tunnelCopy);
            implAccept(s);
	    return s;
/*
            // if verification fails - close the socket
            if (tunnelCopy.verify( ((TunnelSocket)s).getRawInputStream(),  ((TunnelSocket)s).getRawOutputStream() , (Object)s ) ) {

                ((TunnelSocket)s).setUserPrincipal(tunnelCopy.getUserPrincipal());
                ((TunnelSocket)s).setRole(_userBind.getRole());
                ((TunnelSocket)s).setGroup(_userBind.getGroup());
                return s;
            } else {
                s.close();
                continue;
            }
*/
//      }
    }


}
