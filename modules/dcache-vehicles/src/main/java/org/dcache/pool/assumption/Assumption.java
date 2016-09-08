/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

package org.dcache.pool.assumption;

import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.Collection;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.OutOfDateCacheException;

/**
 * An Assumption is an encoding of aspects of a pool under which the pool
 * was selected for a particular task.
 *
 * <p>The Assumption is included with the message submitted to the pool to
 * trigger a particular task, and the pool evaluates the assumption before
 * starting the task. If the assumption fails, the pool rejects the task.</p>
 *
 * <p>The typical use case is for pool manager to encode assumptions is makes
 * about pool load and free space on a pool when selecting it for a transfer.
 * This could for instance be that cost is below a certain threshold (a cost
 * cut). This allows pool manager to select pools without having entirely up
 * to date information about the pool load, yet stil ensure that cost cuts
 * are enforced.</p>
 *
 * <p>Another use case may be for a door to encode that is assumes that a mover
 * can be started without queuing.</p>
 */
public interface Assumption extends Serializable
{
    /**
     * Evaluates the assumption on the given pool.
     *
     * @param pool Provides information about pool load.
     * @return True if and only if the assumption is satisfied.
     */
    boolean isSatisfied(Pool pool);

    /**
     * Throws an exception if the assumption is not satisfied on the given pool.
     *
     * @param pool Provides information about pool load.
     * @throws OutOfDateCacheException If the assumpton is not satisfied.
     */
    default void check(Pool pool) throws OutOfDateCacheException
    {
        if (!isSatisfied(pool)) {
            throw new OutOfDateCacheException("Assumption '" + this + "' failed.");
        }
    }

    /**
     * Returns an assumption that assumes both this and that assumption.
     */
    default Assumption and(Assumption that)
    {
        return that.and(new Assumptions.CompositeAssumption(ImmutableSet.of(this)));
    }

    /**
     * Returns an assumption that assumes both this and those assumptions.
     *
     * For internal use and cannot be called from outside this package.
     */
    default Assumption and(Assumptions.CompositeAssumption those)
    {
        return those.and(new Assumptions.CompositeAssumption(ImmutableSet.of(this)));
    }

    /**
     * Provides access to various load related information of a pool.
     */
    interface Pool
    {
        String name();

        PoolCostInfo.PoolSpaceInfo space();
        double moverCostFactor();

        PoolCostInfo.NamedPoolQueueInfo getMoverQueue(String name);

        Collection<PoolCostInfo.NamedPoolQueueInfo> movers();

        PoolCostInfo.PoolQueueInfo p2PClient();
        PoolCostInfo.PoolQueueInfo p2pServer();
        PoolCostInfo.PoolQueueInfo restore();
        PoolCostInfo.PoolQueueInfo store();
    }
}
