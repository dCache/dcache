package org.dcache.poolmanager;

import java.util.Map;

public class WRandomPartitionFactory implements PartitionFactory
{
    @Override
    public Partition createPartition(Map<String,String> properties) {
        return new WRandomPartition(properties);
    }

    @Override
    public String getDescription() {
        return "Partition with random read and free, and reclaimable space weighted random write selection";
    }

    @Override
    public String getType() {
        return WRandomPartition.TYPE;
    }
}
