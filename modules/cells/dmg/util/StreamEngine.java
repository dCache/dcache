package dmg.util ;
import  java.net.InetAddress ;
import  java.net.Socket ;
import  java.io.InputStream ;
import  java.io.OutputStream ;
import  java.io.Reader ;
import  java.io.Writer ;

import javax.security.auth.Subject;

public interface StreamEngine {

    /**
     *
     * @return {@link Subject} associated with the connections
     */
   public Subject    getSubject() ;

   /**
    *
    * @return Socket object associated with the connections
    */
   public Socket      getSocket() ;

   /**
    *
    * @return local InetAddress
    */
   public InetAddress getLocalAddress();

   /**
    *
    * @return remote InetAddress
    */
   public InetAddress getInetAddress() ;

   /**
    *
    * @return socket input stream
    */
   public InputStream getInputStream() ;

   /**
    *
    * @return return socket output stream
    */
   public OutputStream getOutputStream() ;
   public Reader       getReader() ;
   public Writer       getWriter() ;

    /** Returns the terminal type (vt100, vt220, etc). */
    String getTerminalType();

    /**
     * Returns the terminal width.
     *
     * 0 indicates that the width is unknown.
     */
    int getTerminalWidth();

    /**
     * Returns the terminal height.
     *
     * 0 indicates that the height is unknown.
     */
    int getTerminalHeight();
}
