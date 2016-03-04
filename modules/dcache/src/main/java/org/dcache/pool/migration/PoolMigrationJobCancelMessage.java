package org.dcache.pool.migration;

public class PoolMigrationJobCancelMessage extends PoolMigrationJobMessage
{
    private static final long serialVersionUID = 7250151494463302009L;
    private final boolean _forced;

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
