package diskCacheV111.util;

/**
 * Exception thrown when source pools for pool to pool exceed cost
 * constraints.
 */
public class SourceCostException extends MissingResourceCacheException
{
    public SourceCostException(String message)
    {
        super(message);
    }
}
