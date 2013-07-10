package diskCacheV111.util ;

public class FileNotInCacheException extends CacheException {

    private static final long serialVersionUID = 7790043638464132679L;

     public FileNotInCacheException( String msg ){
        super(FILE_NOT_IN_REPOSITORY, msg);
     }

    public FileNotInCacheException(String msg, Throwable cause) {
        super(FILE_NOT_IN_REPOSITORY, msg, cause);
    }
}
