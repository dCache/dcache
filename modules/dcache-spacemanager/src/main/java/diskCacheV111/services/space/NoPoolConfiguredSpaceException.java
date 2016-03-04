package diskCacheV111.services.space;

/**
 * Request cannot proceed due to a lack of space due to pool-manager
 * configuration.  This exception is the SpaceException equivalent to
 * CacheException.NO_POOL_CONFIGURED.
 */
public class NoPoolConfiguredSpaceException extends NoFreeSpaceException
{
    private static final long serialVersionUID = 6474390713292494503L;

    public NoPoolConfiguredSpaceException(String message)
    {
        super(message);
    }
}
