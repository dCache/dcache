package org.dcache.util;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ForwardingExecutorService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import dmg.cells.nucleus.CDC;

import static com.google.common.collect.Iterables.transform;

/**
 * Decorates a ExecutorService and makes tasks CDC aware.
 *
 * The CDC of a task submitted to the ExecutorService will be initialized to
 * the CDC of the thread that submitted the task.
 */
public class CDCExecutorServiceDecorator extends ForwardingExecutorService
{
    private final ExecutorService _delegate;

    public CDCExecutorServiceDecorator(
            ExecutorService delegate)
    {
        this._delegate = delegate;
    }

    @Override
    protected ExecutorService delegate()
    {
        return _delegate;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task)
    {
        return _delegate.submit(wrap(task));
    }

    @Override
    public Future<?> submit(Runnable task)
    {
        return _delegate.submit(wrap(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result)
    {
        return _delegate.submit(wrap(task), result);
    }

    @Override
    public <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks) throws InterruptedException
    {
        return _delegate.invokeAll(wrap(tasks));
    }

    @Override
    public <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks, long timeout,
            TimeUnit unit) throws InterruptedException
    {
        return _delegate.invokeAll(wrap(tasks), timeout, unit);
    }

    @Override
    public <T> T invokeAny(
            Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
    {
        return _delegate.invokeAny(wrap(tasks));
    }

    @Override
    public <T> T invokeAny(
            Collection<? extends Callable<T>> tasks, long timeout,
            TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        return _delegate.invokeAny(wrap(tasks), timeout, unit);
    }

    @Override
    public void execute(Runnable command)
    {
        _delegate.execute(wrap(command));
    }

    private Runnable wrap(final Runnable task)
    {
        final CDC cdc = new CDC();
        return new Runnable()
        {
            @Override
            public void run()
            {
                cdc.restore();
                try {
                    task.run();
                } finally {
                    CDC.clear();
                }
            }
        };
    }

    private <T> Callable<T> wrap(final Callable<T> task)
    {
        final CDC cdc = new CDC();
        return new Callable<T>() {
            @Override
            public T call() throws Exception
            {
                cdc.restore();
                try {
                    return task.call();
                } finally {
                    cdc.clear();
                }
            }
        };
    }

    private <T> Collection<? extends Callable<T>> wrap(Collection<? extends Callable<T>> tasks)
    {
        return Lists.newArrayList(transform(tasks, new Function<Callable<T>, Callable<T>>()
                {
                    @Override
                    public Callable<T> apply(Callable<T> task)
                    {
                        return wrap(task);
                    }
                }));
    }
}
