package diskCacheV111.util ;

public class FileInCacheException extends CacheException {

    private static final long serialVersionUID = -2554947305070917562L;

    public FileInCacheException( String msg ){
       super( CacheException.FILE_IN_CACHE , msg ) ;
    }
}
