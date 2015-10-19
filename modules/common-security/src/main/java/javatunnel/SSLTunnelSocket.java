/*
 * $Id: SSLTunnelSocket.java,v 1.7 2006-09-05 13:19:53 tigran Exp $
 */

package javatunnel;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.Principal;

import org.dcache.auth.UserNamePrincipal;

public class SSLTunnelSocket extends Socket implements TunnelSocket
{
    private Socket sock;
    private Subject _subject = new Subject();
    private boolean verified;

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
                    Principal principal = new UserNamePrincipal(user);
                    _subject.getPrincipals().add(principal);
                    _subject.setReadOnly();
                }

                verified = true;
            }

        } catch( Exception e) {
            try {
                s.close();
            } catch (IOException ignored ) {}
        }
    }



    @Override
    public OutputStream getOutputStream() throws IOException {
        return sock.getOutputStream();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return sock.getInputStream();
    }

    @Override
    public void close() throws IOException {
        sock.close();
    }


    @Override
    public InetAddress getInetAddress() {
        return sock.getInetAddress();
    }

    @Override
    public int getPort() {
        return sock.getPort();
    }

    public String toString() {
        return sock.toString();
    }

    public void setSubject(Subject subject) {
        _subject = subject;
    }

    @Override
    public boolean verify()
    {
        return verified;
    }

    @Override
    public Subject getSubject() {
        return _subject;
    }
}
