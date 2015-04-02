package diskCacheV111.util ;

/**
 * Thrown when attempting to access a replica that doesn't exist
 * in the repository.
 */
public class FileNotInCacheException extends CacheException {

    private static final long serialVersionUID = 7790043638464132679L;

     public FileNotInCacheException( String msg ){
        super(FILE_NOT_IN_REPOSITORY, msg);
     }

    public FileNotInCacheException(String msg, Throwable cause) {
        super(FILE_NOT_IN_REPOSITORY, msg, cause);
    }
}
