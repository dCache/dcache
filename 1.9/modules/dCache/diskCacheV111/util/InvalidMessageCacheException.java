package diskCacheV111.util;

/**
 * thrown in case of message object incorrectly initialized
 *
 */
public class InvalidMessageCacheException extends CacheException {

    /**
     *
     */
    private static final long serialVersionUID = -8838867882017718935L;

    public InvalidMessageCacheException(String msg) {
        super(CacheException.INVALID_ARGS, msg);
    }

}
