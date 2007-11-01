package dmg.util ;
import  java.net.InetAddress ;
import  java.net.Socket ;
import  java.io.InputStream ;
import  java.io.OutputStream ;
import  java.io.Reader ;
import  java.io.Writer ;

import dmg.security.CellUser;

public interface StreamEngine {

   public CellUser    getUserName() ;
   public Socket      getSocket() ;
   public InetAddress getLocalAddress();
   public InetAddress getInetAddress() ;
   public InputStream getInputStream() ;
   public OutputStream getOutputStream() ;
   public Reader       getReader() ;
   public Writer       getWriter() ;

}
