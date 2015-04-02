package diskCacheV111.util ;

public class InProgressCacheException extends CacheException {

    private static final long serialVersionUID = -6422127441668705469L;

    public InProgressCacheException( int errorCode , String errorMsg ){
        super( errorCode , errorMsg ) ;
    }
}
