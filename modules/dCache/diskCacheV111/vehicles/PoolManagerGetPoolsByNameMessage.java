package diskCacheV111.vehicles;

import java.util.Collection;

public class PoolManagerGetPoolsByNameMessage
    extends PoolManagerGetPoolsMessage
{
    private final Collection<String> _poolNames;

    public PoolManagerGetPoolsByNameMessage(Collection<String> poolNames)
    {
        _poolNames = poolNames;
    }

    public Collection<String> getPoolNames()
    {
        return _poolNames;
    }
}