package diskCacheV111.util;

/**
 * Exception thrown when source pools for pool to pool exceed cost
 * constraints.
 */
public class SourceCostException extends MissingResourceCacheException
{
    private static final long serialVersionUID = -1684204245725575685L;

    public SourceCostException(String message)
    {
        super(message);
    }
}
