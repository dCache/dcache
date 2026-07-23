package diskCacheV111.util;

/**
 * Thrown when attempting to access a file that does not exist in the clients Zone.
 */
public class FileNotInZoneCacheException extends CacheException {

    private static final long serialVersionUID = 7790043638461312161L;

    public FileNotInZoneCacheException(String msg) {
        super(FILE_NOT_IN_REPOSITORY, msg);
    }
}