package diskCacheV111.util ;

public class DirNotExistsCacheException extends CacheException {
    
    private static final long serialVersionUID = 7720043483914132679L;
    
     public DirNotExistsCacheException( String msg ){ 
        super( CacheException.DIR_NOT_EXISTS , msg ) ; 
     }
}
