package diskCacheV111.vehicles;

import java.util.Collection;

public class PoolManagerGetPoolsMessage
    extends Message
{
    private static final long serialVersionUID = 4793574345114253473L;

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