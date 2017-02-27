/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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

import com.google.common.util.concurrent.MoreExecutors;


import java.util.Collection;
import java.util.stream.Collectors;

import diskCacheV111.vehicles.PoolManagerGetPoolsByHsmMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import org.dcache.cells.CellStub;

import static com.google.common.base.Preconditions.checkNotNull;


public class PoolListByHsm
        extends PoolListFromPoolManager
{
    private final CellStub _poolManager;
    private final Collection<String> _hsms;

    public PoolListByHsm(CellStub poolManager, Collection<String> hsms)
    {
        _poolManager = checkNotNull(poolManager);
        _hsms = checkNotNull(hsms);
    }

    @Override
    public void refresh()
    {
        CellStub.addCallback(_poolManager.send(new PoolManagerGetPoolsByHsmMessage(_hsms)),
                this, MoreExecutors.directExecutor());
    }

    @Override
    public String toString()
    {
        return String.format("hsm %s, %d pools",
                _hsms, _pools.size());
    }
}
