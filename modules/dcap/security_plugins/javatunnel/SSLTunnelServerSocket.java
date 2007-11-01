/*
 * $Id: SSLTunnelServerSocket.java,v 1.5 2002-10-22 12:44:43 cvs Exp $
 */

package javatunnel;


import java.io.*;
import java.net.*;

import javax.net.ssl.*;
import javax.net.*;
import javax.net.ssl.*;


import dmg.util.UserValidatable;

public class SSLTunnelServerSocket extends ServerSocket {


	private ServerSocket sock = null;
	private UserValidatable uv = null;

	public SSLTunnelServerSocket(int port, ServerSocketFactory ssf, UserValidatable v) throws java.io.IOException {
	
		sock = ssf.createServerSocket( port );
		uv = v;
	
	}




	public Socket accept() throws java.io.IOException {
		Socket s = new SSLTunnelSocket( sock.accept(), uv );
		return s;
	}


}
