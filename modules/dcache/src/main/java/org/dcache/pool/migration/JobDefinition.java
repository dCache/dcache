package org.dcache.pool.migration;

import java.util.List;
import java.util.Collections;
import java.util.Comparator;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.util.expression.Expression;

/**
 * Immutable record class holding the parameters that define a job.
 */
public class JobDefinition
{
    public final List<CacheEntryFilter> filters;
    public final CacheEntryMode sourceMode;
    public final CacheEntryMode targetMode;
    public final PoolSelectionStrategy selectionStrategy;
    public final Comparator<CacheEntry> comparator;
    public final RefreshablePoolList sourceList;
    public final RefreshablePoolList poolList;
    public final long refreshPeriod;
    public final boolean isPermanent;
    public final boolean isEager;
    public final boolean mustMovePins;
    public final boolean computeChecksumOnUpdate;
    public final Expression pauseWhen;
    public final Expression stopWhen;
    public final boolean forceSourceMode;

    public JobDefinition(List<CacheEntryFilter> filters,
                         CacheEntryMode sourceMode,
                         CacheEntryMode targetMode,
                         PoolSelectionStrategy selectionStrategy,
                         Comparator<CacheEntry> comparator,
                         RefreshablePoolList sourceList,
                         RefreshablePoolList poolList,
                         long refreshPeriod,
                         boolean isPermanent,
                         boolean isEager,
                         boolean mustMovePins,
                         boolean computeChecksumOnUpdate,
                         Expression pauseWhen,
                         Expression stopWhen,
                         boolean forceSourceMode)
    {
        this.filters = Collections.unmodifiableList(filters);
        this.sourceMode = sourceMode;
        this.targetMode = targetMode;
        this.selectionStrategy = selectionStrategy;
        this.comparator = comparator;
        this.sourceList = sourceList;
        this.poolList = poolList;
        this.refreshPeriod = refreshPeriod;
        this.isPermanent = isPermanent;
        this.isEager = isEager;
        this.mustMovePins = mustMovePins;
        this.computeChecksumOnUpdate = computeChecksumOnUpdate;
        this.pauseWhen = pauseWhen;
        this.stopWhen = stopWhen;
        this.forceSourceMode = forceSourceMode;
    }
}