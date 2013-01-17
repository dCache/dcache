package org.dcache.poolmanager;

import java.util.Map;

public class ClassicPartitionFactory implements PartitionFactory
{
    @Override
    public Partition createPartition(Map<String,String> properties)
    {
        return new ClassicPartition(properties);
    }

    @Override
    public String getDescription()
    {
        return "Legacy partition with pool selection based on best cost";
    }

    @Override
    public String getType()
    {
        return ClassicPartition.TYPE;
    }
}
