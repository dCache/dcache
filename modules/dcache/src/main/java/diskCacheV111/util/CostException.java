package diskCacheV111.util;

import org.dcache.poolmanager.PoolInfo;

/**
 * Exception thrown when cost constraints have been exceeded
 * during pool selection.
 */
public class CostException extends MissingResourceCacheException
{
    private static final long serialVersionUID = 2554467702494555943L;
    private final PoolInfo _pool;
    private final boolean _shouldFallBack;
    private final boolean _shouldTryAlternatives;

    public CostException(String message, PoolInfo pool,
                         boolean shouldFallBack,
                         boolean shouldTryAlternatives)
    {
        super(message);
        _pool = pool;
        _shouldFallBack = shouldFallBack;
        _shouldTryAlternatives = shouldTryAlternatives;
    }

    /**
     * Returns a pool for the operation that threw the exception.
     *
     * The pool exceeds cost constraints, but is otherwise a valid
     * pool for the operation. May return null.
     *
     */
    public PoolInfo getPool()
    {
        return _pool;
    }

    /**
     * Indicates if the caller should fallback to lower priority
     * links. Such links may not exist and in such cases fallback
     * is not possible.
     */
    public boolean shouldFallBack()
    {
        return _shouldFallBack;
    }

    /**
     * Whether the caller should try staging or replication
     * instead.
     */
    public boolean shouldTryAlternatives()
    {
        return _shouldTryAlternatives;
    }
}
