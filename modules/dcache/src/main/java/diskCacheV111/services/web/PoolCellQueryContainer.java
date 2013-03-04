// $Id: PoolCellQueryContainer.java,v 1.1 2006-06-05 08:51:27 patrick Exp $Cg

package diskCacheV111.services.web;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

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
    private Map<String,Map<String,Map<String,Object>>> _topology;

    public void put(String name, PoolCellQueryInfo info)
    {
        _infoMap.put(name, info);
    }

    public PoolCellQueryInfo getInfoByName(String name)
    {
        return _infoMap.get(name);
    }

    public void setTopology(Map<String,Map<String,Map<String,Object>>> topology)
    {
        _topology = topology;
    }

    public Set<String> getPoolClassSet()
    {
        return _topology.keySet();
    }

    public Set<String> getPoolGroupSetByClassName(String className)
    {
        Map<String, Map<String, Object>> map = _topology.get(className);
        if (map == null) {
            return null;
        }
        return map.keySet();
    }

    public Map<String,Object>
        getPoolMap(String className, String groupName)
    {
        Map<String, Map<String, Object>> groupMap =
            _topology.get(className);

        if (groupMap == null) {
            return null;
        }

        return groupMap.get(groupName);
    }

    public Map<String,Map<String,Map<String,Object>>>
        getTopology()
    {
        return _topology;
    }

    public String toString()
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
