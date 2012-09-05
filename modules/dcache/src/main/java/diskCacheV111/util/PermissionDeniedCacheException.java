package diskCacheV111.util;

public class PermissionDeniedCacheException extends CacheException
{
    private static final long serialVersionUID = 2922321833396873555L;

    public PermissionDeniedCacheException(String msg)
    {
        super(CacheException.PERMISSION_DENIED, msg);
    }
}
