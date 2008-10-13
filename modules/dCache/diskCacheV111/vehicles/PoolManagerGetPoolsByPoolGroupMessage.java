package diskCacheV111.vehicles;

public class PoolManagerGetPoolsByPoolGroupMessage
    extends PoolManagerGetPoolsMessage
{
    private final String _poolGroup;

    public PoolManagerGetPoolsByPoolGroupMessage(String poolGroup)
    {
        _poolGroup = poolGroup;
    }

    public String getPoolGroup()
    {
        return _poolGroup;
    }
}