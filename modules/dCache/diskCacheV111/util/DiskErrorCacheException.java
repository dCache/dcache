package diskCacheV111.util;

public class DiskErrorCacheException extends CacheException
{
    static final long serialVersionUID = -5386946146340646052L;

    public DiskErrorCacheException(String msg)
    {
        super(CacheException.ERROR_IO_DISK, msg);
    }
}
