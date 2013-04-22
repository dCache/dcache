package diskCacheV111.util;

public class TimeoutCacheException extends CacheException
{
    private static final long serialVersionUID = 3273736524042777488L;

    public TimeoutCacheException(String msg)
    {
        super(CacheException.TIMEOUT, msg);
    }

    public TimeoutCacheException(String message, Throwable cause)
    {
        super(CacheException.TIMEOUT, message, cause);
    }
}
