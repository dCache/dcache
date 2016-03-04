package org.dcache.util;

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

import static java.util.stream.Collectors.toList;

/**
 * Decorates a ExecutorService and makes tasks CDC aware.
 *
 * The CDC of a task submitted to the ExecutorService will be initialized to
 * the CDC of the thread that submitted the task.
 */
public class CDCExecutorServiceDecorator<E extends ExecutorService> extends ForwardingExecutorService
{
    private final E _delegate;

    public CDCExecutorServiceDecorator(E delegate)
    {
        this._delegate = delegate;
    }

    @Override
    public E delegate()
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

    @Override
    public List<Runnable> shutdownNow()
    {
        return unwrap(super.shutdownNow());
    }

    protected Runnable wrap(final Runnable task)
    {
        return new WrappedRunnable(new CDC(), task);
    }

    protected <T> Callable<T> wrap(final Callable<T> task)
    {
        final CDC cdc = new CDC();
        return () -> {
            try (CDC ignored = cdc.restore()) {
                return task.call();
            }
        };
    }

    protected <T> Collection<? extends Callable<T>> wrap(Collection<? extends Callable<T>> tasks)
    {
        return tasks.stream().map(this::wrap).collect(toList());
    }

    private Runnable unwrap(Runnable runnable)
    {
        return (runnable instanceof WrappedRunnable) ? ((WrappedRunnable) runnable).getInner() : runnable;
    }

    private List<Runnable> unwrap(List<Runnable> runnables)
    {
        return runnables.stream().map(this::unwrap).collect(toList());
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
