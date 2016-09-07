package org.dcache.util;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.security.auth.Subject;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;

import org.dcache.auth.attributes.Restriction;

import static com.google.common.base.Preconditions.checkState;

/**
 * A transfer where the mover can send a redirect message to the door
 * asynchronously.
 *
 * The transfer startup phase is identical to a regular Transfer, however notification of
 * redirect and transfer completion is done asynchronously through callbacks. Subclasses
 * are to implement onQueued, onRedirect, onFinish and onFailure. The class deals with out
 * of order notifications and guarantees that:
 *
 *  - onQueued is always called before onRedirect
 *  - onRedirect is always called before onFinish
 *  - that onRedirect and onFinish are not called once onFailure was called
 *
 *  The class implements automatic killing of the mover in case the transfer is
 *  aborted.
 */
public abstract class AsynchronousRedirectedTransfer<T> extends Transfer
{
    private final Executor executor;
    private final Monitor monitor = new Monitor();

    public AsynchronousRedirectedTransfer(Executor executor, PnfsHandler pnfs, Subject namespaceSubject, Restriction restriction, Subject subject, FsPath path) {
        super(pnfs, namespaceSubject, restriction, subject, path);
        this.executor = executor;
    }

    public AsynchronousRedirectedTransfer(Executor executor, PnfsHandler pnfs, Subject subject, Restriction restriction, FsPath path)
    {
        super(pnfs, subject, restriction, path);
        this.executor = executor;
    }

    @Override
    public ListenableFuture<Void> selectPoolAndStartMoverAsync(TransferRetryPolicy policy)
    {
        return monitor.setQueueFuture(super.selectPoolAndStartMoverAsync(policy));
    }

    /**
     * Signals that the transfer is redirected.
     */
    public void redirect(T object)
    {
        executor.execute(() -> monitor.redirect(object));
    }

    /**
     * Aborts the transfer unless already completed.
     *
     * This cancels pool selection and kills the mover. The onFailure callback is called. This method
     * blocks until the callback completes.
     */
    public void abort(Throwable t)
    {
        try {
            FutureTask<Object> task = new FutureTask<>(() -> monitor.doAbort(t), null);
            executor.execute(task);
            task.get();
        } catch (ExecutionException e) {
            Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void finished(CacheException error)
    {
        super.finished(error);
        executor.execute(() -> monitor.finished(error));
    }

    protected abstract void onQueued() throws Exception;

    protected abstract void onRedirect(T object) throws Exception;

    protected abstract void onFinish() throws Exception;

    protected abstract void onFailure(Throwable t);

    /**
     * To avoid locking the monitor of the Transfer object during callbacks,
     * we have an explicit monitor guarding our state machine and ensuring
     * that callbacks are executed sequentially.
     */
    private class Monitor
    {
        private T redirectObject;
        private boolean isQueued;
        private boolean isRedirected;
        private boolean isFinished;
        private boolean isDone;
        private ListenableFuture<Void> queueFuture;

        private synchronized ListenableFuture<Void> setQueueFuture(ListenableFuture<Void> future)
        {
            checkState(queueFuture == null);
            queueFuture = future;
            Futures.addCallback(future, new FutureCallback<Void>()
            {
                @Override
                public void onSuccess(Void result)
                {
                    executor.execute(monitor::doQueued);
                }

                @Override
                public void onFailure(Throwable t)
                {
                    executor.execute(() -> monitor.doAbort(t));
                }
            });
            return future;
        }

        private synchronized void doQueued()
        {
            try {
                isQueued = true;
                if (isDone) {
                    doKill("transfer aborted");
                } else {
                    onQueued();
                    doRedirect();
                }
            } catch (Exception e) {
                doAbort(e);
            }
        }

        private synchronized void doRedirect()
        {
            try {
                if (!isDone && isQueued && isRedirected) {
                    onRedirect(redirectObject);
                    doFinish();
                }
            } catch (Exception e) {
                doAbort(e);
            }
        }

        private synchronized void doFinish()
        {
            try {
                if (!isDone && isQueued && isRedirected && isFinished) {
                    onFinish();
                    isDone = true;
                }
            } catch (Exception e) {
                doAbort(e);
            }
        }

        private synchronized void doAbort(Throwable t)
        {
            if (!isDone) {
                doKill(explain(t));
                onFailure(t);
                isDone = true;
            }
        }

        protected String explain(Throwable t)
        {
            return String.valueOf(t);
        }

        private synchronized void doKill(String explanation)
        {
            if (queueFuture != null) {
                queueFuture.cancel(true);
            }
            killMover(0, "killed by door: " + explanation);
        }

        /**
         * Signals that the transfer is redirected.
         */
        public synchronized void redirect(T object)
        {
            checkState(!isRedirected);
            redirectObject = object;
            isRedirected = true;
            doRedirect();
        }

        public synchronized void finished(CacheException error)
        {
            if (error != null) {
                doAbort(error);
            } else {
                isFinished = true;
                doFinish();
            }
        }
    }
}
