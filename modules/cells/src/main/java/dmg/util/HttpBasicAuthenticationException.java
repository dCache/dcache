// $Id: HttpBasicAuthenticationException.java,v 1.2 2005-03-08 15:37:17 patrick Exp $

package dmg.util ;
import java.io.* ;
import java.util.* ;
/**
  */
public class HttpBasicAuthenticationException extends HttpException  {
    private static final long serialVersionUID = -4829004408849659905L;
    private String _realm;
    public HttpBasicAuthenticationException( String realm ){
       super( 401 , "Unauthorized") ;
       _realm = realm ;
    }
    public String getRealm(){ return _realm ; }
}
