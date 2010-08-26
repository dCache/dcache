package diskCacheV111.util ;

public class FileNotInCacheException extends CacheException {
    
    private static final long serialVersionUID = 7790043638464132679L;
    
     public FileNotInCacheException( String msg ){ 
        super( CacheException.FILE_NOT_IN_REPOSITORY , msg ) ; 
     }
}
