package diskCacheV111.util;

public class NotFileCacheException extends CacheException {

	public NotFileCacheException(String msg) {
		super(CacheException.NOT_FILE, msg);
	}

    public NotFileCacheException(String msg, Throwable cause)
    {
        super(CacheException.NOT_FILE, msg, cause);
    }
}
