/*
 * $Id: SSLTunnelSocket.java,v 1.7 2006-09-05 13:19:53 tigran Exp $
 */

package javatunnel;

import java.io.*;
import java.net.*;

import dmg.util.UserValidatable;

public class SSLTunnelSocket extends Socket implements UserBindible {

	private Socket sock = null;
	private String _user = "nobody@SOME.WHERE";

	SSLTunnelSocket(Socket s, UserValidatable uv ) {
	
		sock = s;
	
		try {
			
			int c;
			int pos = 0;
			byte[] buf = new byte[512];
			InputStream in = sock.getInputStream();
			boolean isGood = true;
			
			do {
				c = in.read();
				if ( c  < 0 ) {
					isGood = false;
					break;
				}
				
				buf[pos] = (byte)c;
				pos ++;
				
			}while(c != '\n');
			
			
			if( isGood ) {
				String auth = new String(buf, 0 , pos-1);
				String user = auth.substring( auth.lastIndexOf('=') +1, auth.lastIndexOf(':'));
				String pass = auth.substring( auth.lastIndexOf(':') +1 );
				
				if( uv.validateUser( user , pass ) ) {
					_user = user  + "@" + getInetAddress().getHostName();
				}								
				
			}
			
		} catch( Exception e) {
			try {
				s.close();
			} catch (IOException ignored ) {}
		}
	}
	
	
	
	public OutputStream getOutputStream() throws java.io.IOException {
		return sock.getOutputStream();
	}
	
	public InputStream getInputStream() throws java.io.IOException {
		return sock.getInputStream();
	}
	
	public void close() throws java.io.IOException {
		sock.close();
	}
 
 
 	public InetAddress getInetAddress() {
		return sock.getInetAddress();
	}
	
	public int getPort() {
		return sock.getPort();
	}
	
	public String toString() {
		return sock.toString();
	}
	
    public void setUserPrincipal(String user) {
        _user = user;
    }    
        
    public String getUserPrincipal() {
        return _user;
    }

	public String getGroup() {		
		return null;
	}

	public String getRole() {
		return null;
	}    

}
