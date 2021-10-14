package org.dcache.util;

import static java.util.stream.Collectors.toList;

import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import dmg.cells.nucleus.CDC;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Decorates a ListeningExecutorService and makes tasks and futures CDC aware.
 * <p>
 * The CDC of a task submitted to the ExecutorService will be initialized to the CDC of the thread
 * that submitted the task. The CDC of any listeners added to a ListenableFuture returned by the
 * decorator will be initialized to the CDC of thread that added the listener.
 */
public class CDCListeningExecutorServiceDecorator extends ForwardingListeningExecutorService {

    private final ListeningExecutorService _delegate;

    public CDCListeningExecutorServiceDecorator(
          ListeningExecutorService delegate) {
        this._delegate = delegate;
    }

    public CDCListeningExecutorServiceDecorator(
          ExecutorService delegate) {
        this(MoreExecutors.listeningDecorator(delegate));
    }

    @Override
    protected ListeningExecutorService delegate() {
        return _delegate;
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task) {
        return wrap(_delegate.submit(wrap(task)));
    }

    @Override
    public ListenableFuture<?> submit(Runnable task) {
        return wrap(_delegate.submit(wrap(task)));
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result) {
        return wrap(_delegate.submit(wrap(task), result));
    }

    @Override
    public <T> List<Future<T>> invokeAll(
          Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return wrap(_delegate.invokeAll(this.<T>wrap(tasks)));
    }

    @Override
    public <T> List<Future<T>> invokeAll(
          Collection<? extends Callable<T>> tasks, long timeout,
          TimeUnit unit) throws InterruptedException {
        return wrap(_delegate.invokeAll(this.<T>wrap(tasks), timeout, unit));
    }

    @Override
    public <T> T invokeAny(
          Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return _delegate.invokeAny(wrap(tasks));
    }

    @Override
    public <T> T invokeAny(
          Collection<? extends Callable<T>> tasks, long timeout,
          TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return _delegate.invokeAny(wrap(tasks), timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        _delegate.execute(wrap(command));
    }

    private <T> ListenableFuture<T> wrap(final ListenableFuture<T> future) {
        return new ForwardingListenableFuture<T>() {
            @Override
            public void addListener(Runnable listener, Executor exec) {
                super.addListener(wrap(listener), exec);
            }

            @Override
            protected ListenableFuture<T> delegate() {
                return future;
            }
        };
    }

    private Runnable wrap(final Runnable task) {
        final CDC cdc = new CDC();
        return () -> {
            try (CDC ignored = cdc.restore()) {
                task.run();
            }
        };
    }

    private <T> Callable<T> wrap(final Callable<T> task) {
        final CDC cdc = new CDC();
        return () -> {
            try (CDC ignored = cdc.restore()) {
                return task.call();
            }
        };
    }

    private <T> Collection<? extends Callable<T>> wrap(Collection<? extends Callable<T>> tasks) {
        return tasks.stream().map(this::wrap).collect(toList());
    }

    private <T> List<Future<T>> wrap(List<Future<T>> futures) {
        return futures.stream().map(future -> wrap((ListenableFuture<T>) future)).collect(toList());
    }
}
