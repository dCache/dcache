package diskCacheV111.util;

public class NotFileCacheException extends CacheException {

        private static final long serialVersionUID = 8192092941871084755L;

        public NotFileCacheException(String msg) {
		super(CacheException.NOT_FILE, msg);
	}

}
