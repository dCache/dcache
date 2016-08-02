/*
 * $Id: SSLServerSocketCreator.java,v 1.4 2002-10-22 12:44:43 cvs Exp $
 */


package javatunnel;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.KeyStore;
import java.util.Map;

import org.dcache.util.Args;

public class SSLServerSocketCreator extends ServerSocketFactory {


    private final SSLServerSocketFactory ssf;
    private UserValidatable uv;

    public  SSLServerSocketCreator(String args, Map<?,UserValidatable> map) throws IOException {
        this(args);
        uv = map.get("UserValidatable");
    }

    public SSLServerSocketCreator(String args) throws IOException {
        this(new Args(args));
    }

    public SSLServerSocketCreator(Args args) throws IOException {

            // args[0] : keystore
            // args[1] : passphrase


            try {
                // set up key manager to do server authentication
                SSLContext ctx;
                KeyManagerFactory kmf;
                KeyStore ks;
                char[] passphrase = null;

                if (args.argv(1) != null) {
                    passphrase = args.argv(1).toCharArray();
                }

                ctx = SSLContext.getInstance("TLS");
                kmf = KeyManagerFactory.getInstance("SunX509");
                ks = KeyStore.getInstance("JKS");

                ks.load(new FileInputStream(args.argv(0)), passphrase);
                kmf.init(ks, passphrase);
                ctx.init(kmf.getKeyManagers(), null, null);

                ssf = ctx.getServerSocketFactory();

            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("ssl failed");
            }

    }


    @Override
    public ServerSocket createServerSocket( int port ) throws IOException {
        return new SSLTunnelServerSocket(port, ssf, uv );
    }

    @Override
    public ServerSocket createServerSocket() throws IOException {
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


    public static void main(String[] args) {
        try{
            SSLServerSocketCreator sc = new SSLServerSocketCreator(new Args(args));
            ServerSocket ss = sc.createServerSocket(1717);
            ss.accept();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
