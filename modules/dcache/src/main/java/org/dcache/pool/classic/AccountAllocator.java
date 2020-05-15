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
import org.dcache.pool.repository.Allocator;

import static java.util.Objects.requireNonNull;

/**
 * An Allocator that is based on some Account object.
 */
public abstract class AccountAllocator implements Allocator
{
    protected final Account _account;

    public AccountAllocator(Account account)
    {
        _account = requireNonNull(account);
    }

    @Override
    public void free(PnfsId id, long space)
    {
        _account.free(id, space);
    }
}
