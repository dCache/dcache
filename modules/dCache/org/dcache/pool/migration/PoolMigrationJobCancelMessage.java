package org.dcache.pool.migration;

public class PoolMigrationJobCancelMessage extends PoolMigrationJobMessage
{
    private boolean _forced;

    public PoolMigrationJobCancelMessage(String id, boolean forced)
    {
        super(id);
        _forced = forced;
    }

    public boolean isForced()
    {
        return _forced;
    }
}