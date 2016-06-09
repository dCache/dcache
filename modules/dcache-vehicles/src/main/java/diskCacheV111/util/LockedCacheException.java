package diskCacheV111.util;

/**
 * Thrown when accessing a locked resource. This is typically a transient
 * error and retrying later maybe allow the operation to succeed without
 * administrative intervention.
 */
public class LockedCacheException extends CacheException
{
    private static final long serialVersionUID = 3557655138424508092L;

    public LockedCacheException(String msg)
    {
        super(CacheException.LOCKED, msg);
    }

    public LockedCacheException(String msg, Throwable cause)
    {
        super(CacheException.LOCKED, msg, cause);
    }
}
