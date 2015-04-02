package diskCacheV111.util ;

public class InconsistentCacheException extends CacheException {

    private static final long serialVersionUID = 2432864584191555957L;

     public InconsistentCacheException( int errorCode , String errorMsg ){
        super( errorCode , errorMsg ) ;
     }
}
