package diskCacheV111.vehicles;

import java.util.Collection;

public class PoolManagerGetPoolsMessage
    extends Message
{
    private Collection<PoolManagerPoolInformation> _pools;

    public void setPools(Collection<PoolManagerPoolInformation> pools)
    {
        _pools = pools;
    }

    public Collection<PoolManagerPoolInformation> getPools()
    {
        return _pools;
    }
}