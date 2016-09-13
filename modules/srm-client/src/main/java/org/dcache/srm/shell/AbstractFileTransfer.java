package org.dcache.srm.shell;

import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Useful base class to represent a transfer.  This implements the callback
 * support and cancellation.
 */
public abstract class AbstractFileTransfer implements FileTransfer
{
    private final SettableFuture<Void> future = SettableFuture.create();

    @Override
    public void addListener(Runnable listener, Executor executor)
    {
        future.addListener(listener, executor);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException
    {
        return future.get();
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException,
            TimeoutException, ExecutionException
    {
        return future.get(timeout, unit);
    }

    @Override
    public boolean isCancelled()
    {
        return future.isCancelled();
    }

    @Override
    public boolean isDone()
    {
        return future.isDone();
    }

    /**
     * Subclass should call this method to mark that the transfer has
     * finished.
     */
    protected synchronized void succeeded()
    {
        future.set(null);
    }

    /**
     * Subclass should call this method to mark that the transfer has
     * failed.
     */
    protected synchronized void failed(Throwable reason)
    {
        future.setException(reason);
    }
}
