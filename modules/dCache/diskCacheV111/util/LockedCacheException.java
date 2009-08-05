package diskCacheV111.util;

public class LockedCacheException extends CacheException
{
    static final long serialVersionUID = 3557655138424508092L;

    public LockedCacheException(String msg)
    {
        super(CacheException.LOCKED, msg);
    }
}
