package diskCacheV111.util;

import static org.dcache.util.Exceptions.genericCheck;

/**
 * Signals that a resource is missing.
 *
 * @since 1.9.3
 */
public class MissingResourceCacheException extends CacheException {

	private static final long serialVersionUID = 4728338447299397298L;

    public static void checkResourceNotMissing(boolean isOK, String format,
            Object...arguments) throws MissingResourceCacheException
    {
        genericCheck(isOK, MissingResourceCacheException::new, format, arguments);
    }

	public MissingResourceCacheException(String msg) {
		super(CacheException.RESOURCE, msg);
	}

    public MissingResourceCacheException(String msg, Throwable cause)
    {
        super(CacheException.RESOURCE, msg, cause);
    }
}
