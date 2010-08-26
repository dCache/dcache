package diskCacheV111.util;

public class NotInTrashCacheException extends CacheException
{
    static final long serialVersionUID = -805746588738612343L;

    public NotInTrashCacheException(String msg)
    {
        super(CacheException.NOT_IN_TRASH, msg);
    }
}
