// $Id: HttpException.java,v 1.2 2005-03-08 15:37:17 patrick Exp $

package dmg.util ;

/**
  */
public class HttpException extends Exception  {
    static final long serialVersionUID = 6807772334572325211L;
    private final int _errorCode ;

    public HttpException( int errorCode , String message ){
       super(message) ;
       _errorCode = errorCode ;
    }
    
    /**  
     * @return HTTP status code
     */
    public int getErrorCode(){ return _errorCode ; }
}
