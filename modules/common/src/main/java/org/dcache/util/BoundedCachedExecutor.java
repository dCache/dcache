/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * A BoundedExecutor that uses a cached thread pool to source threads.
 */
public class BoundedCachedExecutor extends BoundedExecutor
{
    private final ExecutorService executor;

    public BoundedCachedExecutor(int maxThreads)
    {
        this(Executors.defaultThreadFactory(), maxThreads);
    }

    public BoundedCachedExecutor(int maxThreads, int maxQueued)
    {
        this(Executors.defaultThreadFactory(), maxThreads, maxQueued);
    }

    public BoundedCachedExecutor(ThreadFactory threadFactory, int maxThreads)
    {
        this(Executors.newCachedThreadPool(threadFactory), maxThreads);
    }

    public BoundedCachedExecutor(ThreadFactory threadFactory, int maxThreads, int maxQueued)
    {
        this(Executors.newCachedThreadPool(threadFactory), maxThreads, maxQueued);
    }

    protected BoundedCachedExecutor(ExecutorService executor, int maxThreads)
    {
        super(executor, maxThreads);
        this.executor = executor;
    }

    protected BoundedCachedExecutor(ExecutorService executor, int maxThreads, int maxQueued)
    {
        super(executor, maxThreads, maxQueued);
        this.executor = executor;
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        List<Runnable> runnables = super.shutdownNow();
        executor.shutdown();
        return runnables;
    }
}
