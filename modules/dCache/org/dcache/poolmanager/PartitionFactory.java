package org.dcache.poolmanager;

import java.util.Map;

public interface PartitionFactory
{
    Partition createPartition(Map<String,String> properties);
    String getDescription();
    String getType();
}