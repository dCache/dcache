package diskCacheV111.util;

public class OutOfDateCacheException extends CacheException
{
    public OutOfDateCacheException(String msg)
    {
        super(OUT_OF_DATE, msg);
    }
}
