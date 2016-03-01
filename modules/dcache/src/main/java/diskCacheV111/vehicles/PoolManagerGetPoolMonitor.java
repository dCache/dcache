package diskCacheV111.vehicles;

import org.dcache.poolmanager.SerializablePoolMonitor;

public class PoolManagerGetPoolMonitor extends PoolManagerMessage
{
    private static final long serialVersionUID = -378134842673538237L;

    private SerializablePoolMonitor _poolMonitor;

    public PoolManagerGetPoolMonitor()
    {
    }

    public void setPoolMonitor(SerializablePoolMonitor poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    public SerializablePoolMonitor getPoolMonitor()
    {
        return _poolMonitor;
    }
}



