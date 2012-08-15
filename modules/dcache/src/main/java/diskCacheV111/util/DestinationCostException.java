package diskCacheV111.util;

/**
 * Exception thrown when destination pools for pool to pool exceed
 * cost constraints.
 */
public class DestinationCostException extends MissingResourceCacheException
{
    private static final long serialVersionUID = -5730196871505024467L;

    public DestinationCostException(String message)
    {
        super(message);
    }
}
