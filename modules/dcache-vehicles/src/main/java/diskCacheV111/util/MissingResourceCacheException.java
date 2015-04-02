package diskCacheV111.util;

/**
 * Signals that a resource is missing.
 *
 * @since 1.9.3
 */
public class MissingResourceCacheException extends CacheException {

	private static final long serialVersionUID = 4728338447299397298L;

	public MissingResourceCacheException(String msg) {
		super(CacheException.RESOURCE, msg);
	}

    public MissingResourceCacheException(String msg, Throwable cause)
    {
        super(CacheException.RESOURCE, msg, cause);
    }
}
