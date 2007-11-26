/*
 * $Id: CacheException.java,v 1.12 2007-08-30 21:11:05 abaranov Exp $
 */

package diskCacheV111.util ;


/*
 * @Immutable
 */
public class CacheException extends Exception {

    private final int _rc;
    private final String _message;

    public final static int PANIC             = 10000 ;
    public final static int FILE_NOT_FOUND    = 10001 ;
    public final static int FILE_PRECIOUS     = 10002 ;
    public final static int FILESIZE_UNKNOWN  = 10003 ;
    public final static int FILESIZE_MISMATCH = 10004 ;
    public final static int FILE_NOT_STORED   = 10005 ;
    public final static int TIMEOUT           = 10006 ;
    public final static int FILE_NOT_IN_REPOSITORY = 10007;
    public final static int FILE_EXISTS = 10008;
    public final static int DIR_NOT_EXISTS = 10009;
    public final static int NOT_DIR = 10010;
    public final static int UNEXPECTED_SYSTEM_EXCEPTION = 10011;
    public final static int ATTRIBUTE_FORMAT_ERROR = 10012;
    public final static int HSM_DELAY_ERROR = 10013;

    private static final int DEFAULT_ERROR_CODE = 666; // I don't like this number....


    private static final long serialVersionUID = 3219663683702355240L;

    private static String setMessage( String message ){

      StringBuilder sb = new StringBuilder() ;

      for( int i = 0 ; i < message.length() ; i++ ){

         char c = message.charAt(i) ;
         if( c == '\n' ){
            if( i != ( message.length() -1 ) )sb.append(';') ;
         }else{
           sb.append(c) ;
         }
      }
      return sb.toString() ;

    }

    /**
     * Create a new CacheException with default error code and given error message
     * @param msg error message
     */
    public CacheException( String msg ){
    	this(DEFAULT_ERROR_CODE, msg);
    }

    /**
     * Create a new CacheException with given error code and message
     * @param rc error code
     * @param msg error message
     */
    public CacheException( int rc , String msg ){
        _message = setMessage(msg) ;
        _rc = rc ;
    }

    public String getMessage(){ return _message ; }
    public int getRc(){ return _rc ; }
    public String toString(){
        return "CacheException(rc="+_rc+
               ";msg="+getMessage()+")" ;
    }
}

