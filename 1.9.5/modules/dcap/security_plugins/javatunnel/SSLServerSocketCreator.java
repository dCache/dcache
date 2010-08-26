/*
 * $Id: SSLServerSocketCreator.java,v 1.4 2002-10-22 12:44:43 cvs Exp $
 */


package javatunnel;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.KeyStore;
import java.util.Map;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;

import dmg.util.UserValidatable;

public class SSLServerSocketCreator extends ServerSocketFactory {


	private SSLServerSocketFactory ssf = null;
	private UserValidatable uv = null;


	public  SSLServerSocketCreator(String[] args, Map map) throws IOException {
		this(args);
		uv = (UserValidatable)map.get("UserValidatable");
	}


	public SSLServerSocketCreator(String[] args) throws IOException {

		// args[0] : keystore
		// args[1] : passphrase


			try {
				// set up key manager to do server authentication
				SSLContext ctx;
				KeyManagerFactory kmf;
 				KeyStore ks;
 				char[] passphrase = null;

				if( (args.length > 1 ) && (args[1] != null) ){
					passphrase = args[1].toCharArray();
				}

				ctx = SSLContext.getInstance("TLS");
				kmf = KeyManagerFactory.getInstance("SunX509");
				ks = KeyStore.getInstance("JKS");

				ks.load(new FileInputStream(args[0]), passphrase);
				kmf.init(ks, passphrase);
				ctx.init(kmf.getKeyManagers(), null, null);

				ssf = ctx.getServerSocketFactory();

			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException("ssl failed");
			}

	}


    @Override
    public ServerSocket createServerSocket( int port ) throws java.io.IOException {
        return new SSLTunnelServerSocket(port, ssf, uv );
    }

    @Override
    public ServerSocket createServerSocket() throws java.io.IOException {
        return new SSLTunnelServerSocket(ssf, uv );
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog)
            throws IOException {

        return new SSLTunnelServerSocket(port, backlog, ssf, uv);
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog,
            InetAddress ifAddress) throws IOException {

        return new SSLTunnelServerSocket(port, backlog, ifAddress, ssf, uv);
    }


	static public void main(String[] args) {
		try{
			SSLServerSocketCreator sc = new SSLServerSocketCreator(args);
			ServerSocket ss = sc.createServerSocket(1717);
			ss.accept();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
