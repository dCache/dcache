// $Id: PoolCellQueryContainer.java,v 1.1 2006-06-05 08:51:27 patrick Exp $Cg

package diskCacheV111.services.web;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.Set;
import java.io.Serializable;

import diskCacheV111.pools.PoolCellInfo;

class PoolCellQueryInfo implements Serializable
{
    private static final long serialVersionUID = 8645599090478465260L;
    private final PoolCellInfo _poolInfo;
    private final long _pingTime;
    private final long _arrivalTime;

    public PoolCellQueryInfo(PoolCellInfo poolInfo, long pingTime, long arrivalTime)
    {
        _poolInfo = poolInfo;
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
    private SortedMap<String,PoolCellQueryInfo> _infoMap  =
        new TreeMap<>();
    private Map<String,Map<String,Map<String,PoolCellQueryInfo>>> _topology;

    public void put(String name, PoolCellQueryInfo info)
    {
        _infoMap.put(name, info);
    }

    public PoolCellQueryInfo getInfoByName(String name)
    {
        return _infoMap.get(name);
    }

    public void setTopology(Map topology)
    {
        _topology = topology;
    }

    public Set<String> getPoolClassSet()
    {
        return _topology.keySet();
    }

    public Set<String> getPoolGroupSetByClassName(String className)
    {
        Map<String,Map<String,PoolCellQueryInfo>> map =
            _topology.get(className);
        if (map == null) {
            return null;
        }
        return map.keySet();
    }

    public Map<String,PoolCellQueryInfo>
        getPoolMap(String className, String groupName)
    {
        Map<String,Map<String,PoolCellQueryInfo>> groupMap =
            _topology.get(className);

        if (groupMap == null) {
            return null;
        }

        return groupMap.get(groupName);
    }

    public Map<String,Map<String,Map<String,PoolCellQueryInfo>>>
        getTopology()
    {
        return _topology;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String,Map<String,Map<String,PoolCellQueryInfo>>> classes: _topology.entrySet()) {
            String className  = classes.getKey();
            sb.append(" ").append(className).append("\n");

            for (Map.Entry<String,Map<String,PoolCellQueryInfo>> groups:
                     classes.getValue().entrySet()){

                String groupName = groups.getKey();
                sb.append("  ").append(groupName).append("\n");

                for (Map.Entry<String,PoolCellQueryInfo> pools:
                         groups.getValue().entrySet()) {
                    String            poolName = pools.getKey();
                    PoolCellQueryInfo info     = pools.getValue();

                    sb.append("    ").append(poolName)
                        .append(info.toString()).append("\n");
                }
            }
        }

        return sb.toString();
    }
}
