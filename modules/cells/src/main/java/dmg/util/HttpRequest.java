// $Id: HttpRequest.java,v 1.1 2001-09-17 15:08:32 cvs Exp $

package dmg.util ;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;
/**
  */
public interface HttpRequest {

    Map<String,String> getRequestAttributes() ;
    OutputStream getOutputStream() ;
    PrintWriter  getPrintWriter() ;
    String []    getRequestTokens() ;
    int          getRequestTokenOffset() ;
    String getParameter(String parameter);
    boolean      isDirectory() ;
    void         printHttpHeader(int size) ;
    boolean isAuthenticated();
    String getUserName();
    String getPassword();
    void    setContentType(String type) ;
}
