package org.dcache.poolmanager;

import java.util.Map;

public class WassPartitionFactory implements PartitionFactory
{
    @Override
    public Partition createPartition(Map<String,String> properties)
    {
        return new WassPartition(properties);
    }

    @Override
    public String getDescription()
    {
        return "Partition with experimental weighted available space selection";
    }

    @Override
    public String getType()
    {
        return WassPartition.TYPE;
    }
}