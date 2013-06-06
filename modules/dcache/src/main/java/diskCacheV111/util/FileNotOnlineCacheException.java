package diskCacheV111.util ;

/**
 * FileNotOnlineCacheException : file is not online and
 * have to be staged from tape to a pool, but user has no permission
 * to perform staging. Introduced in 'Tape Protection'-feature.
 */
public class FileNotOnlineCacheException extends CacheException {


    private static final long serialVersionUID = -817136788830768272L;

    public FileNotOnlineCacheException( String msg ){
        super( CacheException.FILE_NOT_ONLINE , msg ) ;
     }

    public FileNotOnlineCacheException(String msg, Throwable cause)
    {
        super(CacheException.FILE_NOT_ONLINE, msg, cause);
    }
}
