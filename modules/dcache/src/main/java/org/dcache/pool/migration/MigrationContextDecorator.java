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
package org.dcache.pool.migration;

import java.util.concurrent.ScheduledExecutorService;

import diskCacheV111.util.PnfsId;

import org.dcache.cells.CellStub;
import org.dcache.pool.repository.Repository;

public class MigrationContextDecorator implements MigrationContext
{
    private final MigrationContext inner;

    public MigrationContextDecorator(MigrationContext inner)
    {
        this.inner = inner;
    }

    @Override
    public String getPoolName()
    {
        return inner.getPoolName();
    }

    @Override
    public ScheduledExecutorService getExecutor()
    {
        return inner.getExecutor();
    }

    @Override
    public CellStub getPoolStub()
    {
        return inner.getPoolStub();
    }

    @Override
    public CellStub getPnfsStub()
    {
        return inner.getPnfsStub();
    }

    @Override
    public CellStub getPoolManagerStub()
    {
        return inner.getPoolManagerStub();
    }

    @Override
    public CellStub getPinManagerStub()
    {
        return inner.getPinManagerStub();
    }

    @Override
    public Repository getRepository()
    {
        return inner.getRepository();
    }

    @Override
    public boolean lock(PnfsId pnfsId)
    {
        return inner.lock(pnfsId);
    }

    @Override
    public void unlock(PnfsId pnfsId)
    {
        inner.unlock(pnfsId);
    }

    @Override
    public boolean isActive(PnfsId pnfsId)
    {
        return inner.isActive(pnfsId);
    }
}
