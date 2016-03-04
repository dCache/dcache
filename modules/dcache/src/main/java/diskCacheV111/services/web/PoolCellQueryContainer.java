// $Id: PoolCellQueryContainer.java,v 1.1 2006-06-05 08:51:27 patrick Exp $Cg

package diskCacheV111.services.web;

import com.google.common.collect.Ordering;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import diskCacheV111.pools.PoolCellInfo;

import static com.google.common.base.Preconditions.checkNotNull;

class PoolCellQueryInfo implements Serializable
{
    private static final long serialVersionUID = 8645599090478465260L;
    private final PoolCellInfo _poolInfo;
    private final long _pingTime;
    private final long _arrivalTime;

    public PoolCellQueryInfo(PoolCellInfo poolInfo, long pingTime, long arrivalTime)
    {
        _poolInfo = checkNotNull(poolInfo);
        _pingTime = pingTime;
        _arrivalTime = arrivalTime;
    }

    public String toString()
    {
        return _poolInfo.toString();
    }

    public PoolCellInfo getPoolCellInfo()
    {
        return _poolInfo;
    }

    public long getPingTime()
    {
        return _pingTime;
    }

    public boolean isOk()
    {
        return true;
    }
}

public class PoolCellQueryContainer implements Serializable
{
    private static final long serialVersionUID = 1883299694718571158L;
    private final Map<String,PoolCellQueryInfo> _infoMap = new HashMap<>();
    private Map<String,Map<String,Map<String,Object>>> _topology;

    public synchronized void put(String name, PoolCellQueryInfo info)
    {
        _infoMap.put(name, info);
    }

    public synchronized PoolCellQueryInfo getInfoByName(String name)
    {
        return _infoMap.get(name);
    }

    public synchronized void setTopology(Map<String,Map<String,Map<String,Object>>> topology)
    {
        _topology = topology;
    }

    public synchronized List<String> getPoolClasses()
    {
        return Ordering.natural().sortedCopy(_topology.keySet());
    }

    public synchronized List<String> getPoolGroupSetByClassName(String className)
    {
        Map<String, Map<String, Object>> map = _topology.get(className);
        if (map == null) {
            return null;
        }
        return Ordering.natural().sortedCopy(map.keySet());
    }

    public synchronized Map<String,Object>
        getPoolMap(String className, String groupName)
    {
        Map<String, Map<String, Object>> groupMap =
            _topology.get(className);

        if (groupMap == null) {
            return null;
        }

        return groupMap.get(groupName);
    }

    public synchronized String toString()
    {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Map<String, Map<String, Object>>> classes: _topology.entrySet()) {
            String className  = classes.getKey();
            sb.append(" ").append(className).append("\n");

            for (Map.Entry<String, Map<String, Object>> groups:
                     classes.getValue().entrySet()){

                String groupName = groups.getKey();
                sb.append("  ").append(groupName).append("\n");

                for (Map.Entry<String, Object> pools:
                         groups.getValue().entrySet()) {
                    String            poolName = pools.getKey();
                    Object info     = pools.getValue();

                    sb.append("    ").append(poolName)
                        .append(info.toString()).append("\n");
                }
            }
        }

        return sb.toString();
    }
}
