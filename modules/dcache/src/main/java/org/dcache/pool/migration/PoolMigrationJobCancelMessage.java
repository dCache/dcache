package org.dcache.pool.migration;

import static java.util.Objects.requireNonNull;

public class PoolMigrationJobCancelMessage extends PoolMigrationJobMessage
{
    private static final long serialVersionUID = 7250151494463302009L;
    private final boolean _forced;
    private final String _reason;

    public PoolMigrationJobCancelMessage(String id, boolean forced, String reason)
    {
        super(id);
        _forced = forced;
        _reason = requireNonNull(reason);
    }

    public boolean isForced()
    {
        return _forced;
    }

    public String getReason()
    {
        return _reason;
    }
}
