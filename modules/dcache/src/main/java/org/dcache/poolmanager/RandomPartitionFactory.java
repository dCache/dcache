package org.dcache.poolmanager;

import java.util.Map;

public class RandomPartitionFactory implements PartitionFactory
{
    @Override
    public Partition createPartition(Map<String,String> properties)
    {
        return new RandomPartition(properties);
    }

    @Override
    public String getDescription()
    {
        return "Selects pools randomly";
    }

    @Override
    public String getType()
    {
        return RandomPartition.TYPE;
    }
}