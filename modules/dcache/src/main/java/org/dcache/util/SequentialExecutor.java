package org.dcache.util;

import java.util.concurrent.Executor;

/**
 * An executor which executes its tasks sequentially.
 *
 * Differs from Executors#newSingleThreadExecutor by sourcing a thread
 * from a shared executor when needed.
 */
public class SequentialExecutor extends BoundedExecutor
{
    public SequentialExecutor(Executor executor)
    {
        super(executor, 1);
    }
}
