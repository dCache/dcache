package org.dcache.poolmanager;

import java.util.Map;

public class WRandomPartitionFactory implements PartitionFactory {

    @Override
    public Partition createPartition(Map<String, String> properties) {
        return new WRandom(properties);
    }

    @Override
    public String getDescription() {
        return "Partirion with random read and free + reclaimable space weighted random write selection.";
    }

    @Override
    public String getType() {
        return WRandom.TYPE;
    }

}
