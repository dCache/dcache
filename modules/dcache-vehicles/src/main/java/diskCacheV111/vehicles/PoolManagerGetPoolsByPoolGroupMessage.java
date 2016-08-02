package diskCacheV111.vehicles;

import static com.google.common.base.Preconditions.checkNotNull;

public class PoolManagerGetPoolsByPoolGroupMessage
    extends PoolManagerGetPoolsMessage
{
    private static final long serialVersionUID = 2808625734157545379L;

    private final Iterable<String> _poolGroups;

    public PoolManagerGetPoolsByPoolGroupMessage(Iterable<String> poolGroups)
    {
        _poolGroups = checkNotNull(poolGroups);
    }

    public Iterable<String> getPoolGroups()
    {
        return _poolGroups;
    }
}
