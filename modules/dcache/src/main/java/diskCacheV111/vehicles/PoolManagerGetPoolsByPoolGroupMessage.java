package diskCacheV111.vehicles;

public class PoolManagerGetPoolsByPoolGroupMessage
    extends PoolManagerGetPoolsMessage
{
    private static final long serialVersionUID = 2808625734157545379L;

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