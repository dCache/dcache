// $Id: CacheException.java,v 1.8 2006-05-17 12:23:30 tigran Exp $

package diskCacheV111.util ;


 
public class CacheException extends Exception {

    private int _rc = 666 ;
    private String _message = null ;

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
    
    private static final long serialVersionUID = 3219663683702355240L;
    
    private void setMessage( String message ){
      StringBuffer sb = new StringBuffer() ;
      for( int i = 0 ; i < message.length() ; i++ ){
        
         char c = message.charAt(i) ;
         if( c == '\n' ){
            if( i != ( message.length() -1 ) )sb.append(';') ;
         }else{
           sb.append(c) ;
         }
      }
      _message = sb.toString() ;
    
    }
    public CacheException( String msg ){ setMessage( msg ) ; }
   
    public CacheException( int rc , String msg ){
        setMessage(msg) ;
        _rc = rc ;
    }
    public String getMessage(){ return _message ; } 
    public int getRc(){ return _rc ; }
    public String toString(){ 
        return "CacheException(rc="+_rc+
               ";msg="+getMessage()+")" ;
    }
}
 
