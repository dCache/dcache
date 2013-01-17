package org.dcache.pool.migration;

import diskCacheV111.vehicles.Message;

public class PoolMigrationJobMessage extends Message
{
    private static final long serialVersionUID = -1581134399801361981L;
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
