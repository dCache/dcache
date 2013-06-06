package diskCacheV111.util;

public class NotInTrashCacheException extends CacheException
{
    private static final long serialVersionUID = -805746588738612343L;

    public NotInTrashCacheException(String msg)
    {
        super(CacheException.NOT_IN_TRASH, msg);
    }

    public NotInTrashCacheException(String msg, Throwable cause)
    {
        super(CacheException.NOT_IN_TRASH, msg, cause);
    }
}
