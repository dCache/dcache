/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.pool.migration;

import java.util.concurrent.ScheduledExecutorService;

import org.dcache.cells.CellStub;

/**
 * Migration task parameters that do not pertain to an individual file.
 * These parameters are often in common for many tasks (e.g. all tasks
 * thar belong to a single Job).
 */
public class TaskParameters
{
    /**
     * Generic communication stub to talk to a destination pool.
     */
    public final CellStub pool;

    /**
     * Communication stub to talk to pnfsmanager.
     */
    public final CellStub pnfs;

    /**
     * Communication stub to talk to pinmanager.
     */
    public final CellStub pinManager;

    /**
     * Scheduled executor to be used by the task for asynchronous operations.
     */
    public final ScheduledExecutorService executor;

    /**
     * Strategy object for selection a target pool.
     */
    public final PoolSelectionStrategy selectionStrategy;

    /**
     * Pool information about target pools.
     */
    public final RefreshablePoolList poolList;

    /**
     * Whether the job is eager. Eager jobs proceed by creating new replicas if
     * existing replicas are inaccessible.
     */
    public final boolean isEager;

    /**
     * Wether the job will only copy meta data to existing replicas or create
     * new replicas.
     */
    public final boolean isMetaOnly;

    /**
     * Whether to verify the checksum when reusing existing target replicas.
     */
    public final boolean computeChecksumOnUpdate;

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

    public TaskParameters(CellStub pool, CellStub pnfs, CellStub pinManager, ScheduledExecutorService executor,
                          PoolSelectionStrategy selectionStrategy, RefreshablePoolList poolList, boolean isEager,
                          boolean isMetaOnly, boolean computeChecksumOnUpdate, boolean forceSourceMode,
                          boolean maintainAtime, int replicas)
    {
        this.pool = pool;
        this.pnfs = pnfs;
        this.pinManager = pinManager;
        this.executor = executor;
        this.selectionStrategy = selectionStrategy;
        this.poolList = poolList;
        this.isEager = isEager;
        this.isMetaOnly = isMetaOnly;
        this.computeChecksumOnUpdate = computeChecksumOnUpdate;
        this.forceSourceMode = forceSourceMode;
        this.maintainAtime = maintainAtime;
        this.replicas = replicas;
    }
}
