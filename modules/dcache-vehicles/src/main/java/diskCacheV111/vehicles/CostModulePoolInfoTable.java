package diskCacheV111.vehicles;

import diskCacheV111.pools.PoolCostInfo;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * An Hashtable with the timestamping feature.<br/> This table is designed to contain PoolCostInfo
 * objects. In this table there are all information about all pools, then the pool's state at a
 * given time (recorded by the timestamp). The key is the name of the pool as String.
 *
 * @author Nicolò Fioretti
 * @see Hashtable
 * @see PoolCostInfo
 */
public class CostModulePoolInfoTable implements Serializable {

    private final long _timestamp = System.currentTimeMillis();
    private final Map<String, PoolCostInfo> _table = new HashMap<>();

    private static final long serialVersionUID = -8035876156296337291L;


    public long getTimestamp() {
        return _timestamp;
    }

    public String toString() {
        return "Pools Space Information: " + getTimestamp();
    }

    public void addPoolCostInfo(String poolName, PoolCostInfo info) {
        _table.put(poolName, info);
    }

    public PoolCostInfo getPoolCostInfoByName(String poolName) {
        return _table.get(poolName);
    }

    public Collection<PoolCostInfo> poolInfos() {
        return _table.values();
    }


}
