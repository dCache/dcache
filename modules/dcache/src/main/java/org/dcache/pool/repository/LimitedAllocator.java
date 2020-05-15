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
package org.dcache.pool.repository;

import diskCacheV111.util.PnfsId;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * An Allocator that wraps some other Allocator and imposes a limit on
 * the total allocation for this upload.
 */
public class LimitedAllocator extends ForwardingAllocator
{
    private final Allocator _inner;
    private final long _maximumSize;
    private long _currentSize;

    public LimitedAllocator(Allocator inner, long maximumSize)
    {
        _inner = inner;
        _maximumSize = maximumSize;
    }

    @Override
    protected Allocator getAllocator()
    {
        return _inner;
    }

    @Override
    public synchronized void allocate(PnfsId id, long size) throws IllegalStateException,
            IllegalArgumentException, InterruptedException, OutOfDiskException
    {
        checkArgument(size >= 0);
        if (_currentSize + size > _maximumSize) {
            throw new OutOfDiskException("Exceeded allowed upload size");
        }
        super.allocate(id, size);
        _currentSize += size;
    }

    @Override
    public synchronized void free(PnfsId id, long size) throws IllegalStateException, IllegalArgumentException
    {
        checkArgument(size >= 0);
        super.free(id, size);
        _currentSize -= size;
    }
}
