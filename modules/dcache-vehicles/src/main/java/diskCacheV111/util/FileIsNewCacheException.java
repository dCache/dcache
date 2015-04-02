package diskCacheV111.util;

/**
 * An operation failed because the file is still "new", ie. has not
 * finished uploading and registered the file in the name space.
 */
public class FileIsNewCacheException extends CacheException
{
    private static final long serialVersionUID = -5278591371301476563L;

    public FileIsNewCacheException()
    {
        this("File upload not yet completed");
    }

    public FileIsNewCacheException(String s)
    {
        super(CacheException.FILE_IS_NEW, s);
    }
}
