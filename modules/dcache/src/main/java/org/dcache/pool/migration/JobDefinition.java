package org.dcache.pool.migration;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.util.expression.Expression;

/**
 * Immutable record class holding the parameters that define a job.
 */
public class JobDefinition
{
    /**
     * Selection criteria defining which replicas to include in the migration job.
     */
    public final List<CacheEntryFilter> filters;

    /**
     * New mode of the source replica to apply after successful migration.
     */
    public final CacheEntryMode sourceMode;

    /**
     * Mode of the target replica to apply after successful migration.
     */
    public final CacheEntryMode targetMode;

    /**
     * Strategy object for selection a target pool.
     */
    public final PoolSelectionStrategy selectionStrategy;

    /**
     * Sort order for the initial replica list.
     */
    public final Comparator<CacheEntry> comparator;

    /**
     * Pool information about the source pool. Must contain exactly one
     * pool.
     */
    public final RefreshablePoolList sourceList;

    /**
     * Pool information about target pools.
     */
    public final RefreshablePoolList poolList;

    /**
     * How often to refresh pool information.
     */
    public final long refreshPeriod;

    /**
     * Whether the job is permanent. Permanent jobs add new replicas to the job if
     * they match the selection criteria.
     */
    public final boolean isPermanent;

    /**
     * Whether the job is eager. Eager jobs proceed by creating new replicas if
     * existing replicas are inaccessible.
     */
    public final boolean isEager;

    /**
     * Whether to move pins to the target pool after successful migration.
     */
    public final boolean mustMovePins;

    /**
     * Whether to verify the checksum when reusing existing target replicas.
     */
    public final boolean computeChecksumOnUpdate;

    /**
     * Condition when to pause the migration job.
     */
    public final Expression pauseWhen;

    /**
     * Condition when to stop the migration job.
     */
    public final Expression stopWhen;

    /**
     * Whether the migration job overrides the mode of the source pool, that is,
     * whether to allow migration even if the source pool is disabled.
     */
    public final boolean forceSourceMode;

    /**
     * Whether to maintain the last access time on copy.
     */
    public final boolean maintainAtime;

    /**
     * Number of replicas to create.
     */
    public final int replicas;

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
                         int replicas,
                         boolean mustMovePins,
                         boolean computeChecksumOnUpdate,
                         boolean maintainAtime,
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
        this.replicas = replicas;
        this.mustMovePins = mustMovePins;
        this.computeChecksumOnUpdate = computeChecksumOnUpdate;
        this.maintainAtime = maintainAtime;
        this.pauseWhen = pauseWhen;
        this.stopWhen = stopWhen;
        this.forceSourceMode = forceSourceMode;
    }
}
