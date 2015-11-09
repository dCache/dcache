package org.dcache.util;

import com.google.common.collect.ForwardingObject;

import java.util.concurrent.Executor;

import dmg.cells.nucleus.CDC;

/**
 * Decorates an Executor and makes tasks CDC aware.
 *
 * The CDC of a task submitted to the Executor will be initialized to
 * the CDC of the thread that submitted the task.
 */
public class CDCExecutorDecorator<E extends Executor> extends ForwardingObject
    implements Executor
{
    private final E _delegate;

    public CDCExecutorDecorator(E delegate)
    {
        this._delegate = delegate;
    }

    @Override
    public E delegate()
    {
        return _delegate;
    }

    @Override
    public void execute(Runnable command)
    {
        _delegate.execute(wrap(command));
    }

    protected Runnable wrap(final Runnable task)
    {
        return new WrappedRunnable(new CDC(), task);
    }

    private static class WrappedRunnable implements Runnable
    {
        private final CDC cdc;
        private final Runnable task;

        public WrappedRunnable(CDC cdc, Runnable task)
        {
            this.cdc = cdc;
            this.task = task;
        }

        public Runnable getInner()
        {
            return task;
        }

        @Override
        public void run()
        {
            try (CDC ignored = cdc.restore()) {
                task.run();
            }
        }
    }
}
