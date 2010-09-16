package org.dcache.pool.migration;

import diskCacheV111.vehicles.Message;

public class PoolMigrationJobMessage extends Message
{
    private final String _id;

    public PoolMigrationJobMessage(String id)
    {
        _id = id;
    }

    public String getJobId()
    {
        return _id;
    }
}