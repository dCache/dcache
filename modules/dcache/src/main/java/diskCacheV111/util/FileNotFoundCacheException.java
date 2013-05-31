package diskCacheV111.util ;

public class FileNotFoundCacheException extends CacheException {

    private static final long serialVersionUID = 7720043483914132679L;

    public FileNotFoundCacheException( String msg ){
        super( 10001 , msg ) ;
    }

    public FileNotFoundCacheException( String msg, Throwable cause){
        super( 10001 , msg, cause ) ;
    }
}
