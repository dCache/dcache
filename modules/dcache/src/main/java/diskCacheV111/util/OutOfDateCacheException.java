package diskCacheV111.util;

public class OutOfDateCacheException extends CacheException
{
    private static final long serialVersionUID = 7169163632355090575L;

    public OutOfDateCacheException(String msg)
    {
        super(OUT_OF_DATE, msg);
    }
}
