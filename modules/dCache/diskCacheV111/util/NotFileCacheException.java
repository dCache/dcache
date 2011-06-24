package diskCacheV111.util;

public class NotFileCacheException extends CacheException {

	public NotFileCacheException(String msg) {
		super(CacheException.NOT_FILE, msg);
	}

}
