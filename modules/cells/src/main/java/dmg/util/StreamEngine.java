package dmg.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.InetAddress;
import java.net.Socket;
import javax.security.auth.Subject;

public interface StreamEngine {

    /**
     * @return {@link Subject} associated with the connections
     */
    Subject getSubject();

    /**
     * @return Socket object associated with the connections
     */
    Socket getSocket();

    /**
     * @return local InetAddress
     */
    InetAddress getLocalAddress();

    /**
     * @return remote InetAddress
     */
    InetAddress getInetAddress();

    /**
     * @return socket input stream
     */
    InputStream getInputStream();

    /**
     * @return return socket output stream
     */
    OutputStream getOutputStream();

    Reader getReader();

    Writer getWriter();
}
