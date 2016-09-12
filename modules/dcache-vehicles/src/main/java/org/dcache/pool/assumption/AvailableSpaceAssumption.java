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

import diskCacheV111.pools.PoolCostInfo;

/**
 * An assumption on the available space in a pool.
 *
 * Available space is the sum of free and removable space. The assumption
 * fails if a pool has less available space than assumed.
 */
public class AvailableSpaceAssumption implements Assumption
{
    private static final long serialVersionUID = -8945173816059261047L;

    private final long limit;

    public AvailableSpaceAssumption(long limit)
    {
        this.limit = limit;
    }

    @Override
    public boolean isSatisfied(Pool pool)
    {
        PoolCostInfo.PoolSpaceInfo space = pool.space();
        return space.getFreeSpace() + space.getRemovableSpace() - space.getGap() >= limit;
    }

    @Override
    public String toString()
    {
        return "available space is at least " + limit;
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

        AvailableSpaceAssumption that = (AvailableSpaceAssumption) o;
        return limit == that.limit;

    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(limit);
    }
}
