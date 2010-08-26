// $Id: HttpRequest.java,v 1.1 2001-09-17 15:08:32 cvs Exp $

package dmg.util ;
import java.io.* ;
import java.util.* ;
/**
  */
public interface HttpRequest {

    public Map<String,String> getRequestAttributes() ;
    public OutputStream getOutputStream() ;
    public PrintWriter  getPrintWriter() ;
    public String []    getRequestTokens() ;
    public int          getRequestTokenOffset() ;
    public boolean      isDirectory() ;
    public void         printHttpHeader( int size ) ;
    public boolean isAuthenticated();
    public String getUserName();
    public String getPassword();
    public void    setContentType( String type ) ;
}
