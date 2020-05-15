/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.classic;

import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.OutOfDiskException;

/**
 * An Allocator that throws OutOfDiskException if there is insufficient space
 * for the requested capacity.  This strategy fails uploads quickly, to avoid
 * blocking IO operations for any extended period.
 */
public class ImmediateAllocator extends AccountAllocator
{
    public ImmediateAllocator(Account account)
    {
        super(account);
    }

    @Override
    public void allocate(PnfsId id, long size) throws InterruptedException, OutOfDiskException
    {
        if (!_account.allocateNow(id, size)) {
            throw new OutOfDiskException("Out of space");
        }
    }
}
