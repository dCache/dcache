package diskCacheV111.services.space;

public class SpaceAuthorizationException extends SpaceException{

    private static final long serialVersionUID = 8416442941231170858L;

    /** Creates a new instance of SpaceException */
    public SpaceAuthorizationException() {
        super();
    }

    public SpaceAuthorizationException(String message) {
        super(message);
    }

    public SpaceAuthorizationException(String message,Throwable cause) {
        super(message,cause);
    }

    public SpaceAuthorizationException(Throwable cause) {
        super(cause);
    }

}
