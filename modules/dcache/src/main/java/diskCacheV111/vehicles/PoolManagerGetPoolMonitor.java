package diskCacheV111.vehicles;

import diskCacheV111.poolManager.PoolMonitorV5;

public class PoolManagerGetPoolMonitor extends PoolManagerMessage
{
    static final long serialVersionUID = -378134842673538237L;

    private PoolMonitorV5 _poolMonitor;

    public PoolManagerGetPoolMonitor()
    {
    }

    public void setPoolMonitor(PoolMonitorV5 poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    public PoolMonitorV5 getPoolMonitor()
    {
        return _poolMonitor;
    }
}



