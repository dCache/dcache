package diskCacheV111.util;

/**
 * Exception thrown when destination pools for pool to pool exceed
 * cost constraints.
 */
public class DestinationCostException extends MissingResourceCacheException
{
    public DestinationCostException(String message)
    {
        super(message);
    }
}
