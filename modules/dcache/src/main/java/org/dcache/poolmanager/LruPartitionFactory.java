package org.dcache.poolmanager;

import java.util.Map;

public class LruPartitionFactory implements PartitionFactory
{
    @Override
    public Partition createPartition(Map<String,String> properties)
    {
        return new LruPartition(properties);
    }

    @Override
    public String getDescription()
    {
        return "Selects least recently used pool";
    }

    @Override
    public String getType()
    {
        return LruPartition.TYPE;
    }
}
