/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 - 2020 Deutsches Elektronen-Synchrotron
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

import static java.util.Objects.requireNonNull;

import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.vehicles.PoolManagerGetPoolsByHsmMessage;
import java.util.Collection;
import org.dcache.cells.CellStub;


public class PoolListByHsm
      extends PoolListFromPoolManager {

    private final CellStub _poolManager;
    private final Collection<String> _hsms;

    public PoolListByHsm(CellStub poolManager, Collection<String> hsms) {
        _poolManager = requireNonNull(poolManager);
        _hsms = requireNonNull(hsms);
    }

    @Override
    public void refresh() {
        CellStub.addCallback(_poolManager.send(new PoolManagerGetPoolsByHsmMessage(_hsms)),
              this, MoreExecutors.directExecutor());
    }

    @Override
    public String toString() {
        return String.format("hsm %s, %d pools",
              _hsms, _pools.size());
    }
}
