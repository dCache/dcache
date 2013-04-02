package diskCacheV111.util;

public class DiskErrorCacheException extends CacheException
{
    private static final long serialVersionUID = -5386946146340646052L;

    public DiskErrorCacheException(String msg)
    {
        super(CacheException.ERROR_IO_DISK, msg);
    }

    public DiskErrorCacheException(String message, Exception cause)
    {
        super(CacheException.ERROR_IO_DISK, message, cause);
    }
}
