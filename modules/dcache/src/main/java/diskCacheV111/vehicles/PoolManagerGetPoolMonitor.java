package diskCacheV111.vehicles;

import org.dcache.poolmanager.PoolMonitor;

public class PoolManagerGetPoolMonitor extends PoolManagerMessage
{
    private static final long serialVersionUID = -378134842673538237L;

    private PoolMonitor _poolMonitor;

    public PoolManagerGetPoolMonitor()
    {
    }

    public void setPoolMonitor(PoolMonitor poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    public PoolMonitor getPoolMonitor()
    {
        return _poolMonitor;
    }
}



