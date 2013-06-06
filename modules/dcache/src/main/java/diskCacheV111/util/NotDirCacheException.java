package diskCacheV111.util ;

public class NotDirCacheException extends CacheException {

    private static final long serialVersionUID = 7720043483914132679L;

     public NotDirCacheException( String msg ){
        super( CacheException.NOT_DIR , msg ) ;
     }

    public NotDirCacheException(String msg, Throwable cause)
    {
        super(CacheException.NOT_DIR, msg, cause);
    }
}
