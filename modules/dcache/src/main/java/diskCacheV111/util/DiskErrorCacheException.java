package diskCacheV111.util;

/**
 * Signals that an error with a pool's disk has occurred.
 *
 * The exception may indicate a hardware failure, software failure,
 * or another process tampering with dCache's files.
 *
 * This exception is different from Java's IOException in that a
 * DiskErrorCacheException always indicates a problem with the
 * disk/file system, while IOException can be any I/O error,
 * eg a network error.
 *
 * The pool will usually consider this exception fatal and disable
 * the pool.
 */
public class DiskErrorCacheException extends CacheException
{
    private static final long serialVersionUID = -5386946146340646052L;

    public DiskErrorCacheException(String msg)
    {
        super(CacheException.ERROR_IO_DISK, msg);
    }

    public DiskErrorCacheException(String message, Throwable cause)
    {
        super(CacheException.ERROR_IO_DISK, message, cause);
    }
}
