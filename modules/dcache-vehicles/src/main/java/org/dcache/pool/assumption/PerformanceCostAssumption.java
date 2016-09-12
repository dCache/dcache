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

import java.util.stream.DoubleStream;

import diskCacheV111.pools.PoolCostInfo;

/**
 * An assumption on the performance cost of a pool.
 *
 * The performance cost is defined as the average fill factor of the mover
 * queues and the HSM store queue. The assumption fails of the performance
 * cost is higher than the assumed.
 */
public class PerformanceCostAssumption implements Assumption
{
    private static final long serialVersionUID = 1511067206767819221L;

    private final double limit;

    private PerformanceCostAssumption(double limit)
    {
        this.limit = limit;
    }

    @Override
    public boolean isSatisfied(Pool pool)
    {
        return PoolCostInfo.getPerformanceCost(pool.store(), pool.movers()) <= limit;
    }

    @Override
    public String toString()
    {
        return "performance cost is less than " + limit;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PerformanceCostAssumption that = (PerformanceCostAssumption) o;
        return Double.compare(that.limit, limit) == 0;
    }

    @Override
    public int hashCode()
    {
        return Double.hashCode(limit);
    }

    /**
     * Creates an assumption on the performance cost of a pool.
     *
     * The assumption is formed from the smallest of a number of positive finite limits. A error
     * margin is added to the limit to allow the pool to overshoot slightly without failing the
     * assumption.
     *
     * @param error The margin of error to add to the limit
     * @param limits Zero or more limits; non-positive or non-finite values are discarded
     */
    public static Assumption of(double error, double... limits)
    {
        double limit = DoubleStream.of(limits).filter(l -> l > 0.0).min().orElse(Double.POSITIVE_INFINITY);
        return Double.isFinite(limit) ? new PerformanceCostAssumption(limit + error) : Assumptions.none();
    }
}
