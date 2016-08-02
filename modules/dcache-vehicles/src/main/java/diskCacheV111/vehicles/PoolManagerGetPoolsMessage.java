package diskCacheV111.vehicles;

import java.util.Collection;
import java.util.Collections;

public abstract class PoolManagerGetPoolsMessage
    extends Message
{
    private static final long serialVersionUID = 4793574345114253473L;

    private Collection<PoolManagerPoolInformation> _pools;
    private Collection<String> _offlinePools;

    public void setPools(Collection<PoolManagerPoolInformation> pools)
    {
        _pools = pools;
    }

    public void setOfflinePools(Collection<String> pools)
    {
        _offlinePools = pools;
    }

    public Collection<PoolManagerPoolInformation> getPools()
    {
        return _pools;
    }

    public Collection<String> getOfflinePools()
    {
        return (_offlinePools == null) ? Collections.emptyList() : _offlinePools;
    }
}
