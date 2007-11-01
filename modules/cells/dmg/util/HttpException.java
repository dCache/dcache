// $Id: HttpException.java,v 1.2 2005-03-08 15:37:17 patrick Exp $

package dmg.util ;
import java.io.* ;
import java.util.* ;
/**
  */
public class HttpException extends Exception  {
    static final long serialVersionUID = 6807772334572325211L;
    private int _errorCode = 0 ;
    public HttpException( int errorCode , String message ){
       super(message) ;
       _errorCode = errorCode ;
    }
    public int getErrorCode(){ return _errorCode ; }
}
