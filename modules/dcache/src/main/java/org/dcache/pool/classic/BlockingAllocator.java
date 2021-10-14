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

/**
 * An Allocator that blocks if there is insufficient space.  This strategy will accept data and rely
 * on the admin to shuffle data around if there is insufficient capacity.
 */
public class BlockingAllocator extends AccountAllocator {

    public BlockingAllocator(Account account) {
        super(account);
    }

    @Override
    public void allocate(PnfsId id, long size) throws InterruptedException {
        _account.allocate(id, size);
    }
}
