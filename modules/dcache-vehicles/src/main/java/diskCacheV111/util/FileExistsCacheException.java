package diskCacheV111.util ;

public class FileExistsCacheException extends CacheException {

    private static final long serialVersionUID = 7720043483914132679L;

     public FileExistsCacheException( String msg ){
        super( CacheException.FILE_EXISTS , msg ) ;
     }

    public FileExistsCacheException(String msg, Throwable cause)
    {
        super(CacheException.FILE_EXISTS, msg, cause);
    }
}
