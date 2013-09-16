package diskCacheV111.util ;

public class FileNotFoundCacheException extends CacheException {

    private static final long serialVersionUID = 7720043483914132679L;

    public FileNotFoundCacheException( String msg ){
        super(FILE_NOT_FOUND, msg);
    }

    public FileNotFoundCacheException( String msg, Throwable cause){
        super(FILE_NOT_FOUND, msg, cause);
    }
}
