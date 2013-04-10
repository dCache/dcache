package org.dcache.pool.classic;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dmg.cells.nucleus.CDC;

/**
 *
 * @since 1.9.11
 */
public class LegacyMoverExecutorService implements MoverExecutorService
{
    private final static Logger _log =
        LoggerFactory.getLogger(LegacyMoverExecutorService.class);
    private final static String _name =
        LegacyMoverExecutorService.class.getSimpleName();

    private final ExecutorService _executor =
            Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat(_name + "-worker-%d").build());

    @Override
    public Cancellable execute(PoolIORequest request, CompletionHandler<Void,Void> completionHandler)
    {
        MoverTask task = new MoverTask(request, completionHandler);
        _executor.execute(task);
        return task;
    }

    public void shutdown()
    {
        _executor.shutdown();
    }

    private static class MoverTask implements Runnable, Cancellable
    {
        private final PoolIORequest _request;
        private final CDC _cdc = new CDC();
        private final CompletionHandler<Void,Void> _completionHandler;

        private Thread _thread;
        private boolean _needInterruption;

        public MoverTask(PoolIORequest request, CompletionHandler<Void,Void> completionHandler) {
            _request = request;
            _completionHandler = completionHandler;
        }

        @Override
        public void run() {
            try {
                setThread();
                _cdc.restore();
                try {
                    _request.getTransfer().transfer();
                } catch (Throwable t) {
                    _completionHandler.failed(t, null);
                    throw t;
                }
                _completionHandler.completed(null, null);
            } catch (RuntimeException e) {
                _log.error("Transfer failed due to a bug: {}", e);
            } catch (Exception e) {
                _log.error("Transfer failed: {}", e.toString());
            } catch (Throwable e) {
                _log.error("Transfer failed:", e);
                Thread t = Thread.currentThread();
                t.getUncaughtExceptionHandler().uncaughtException(t, e);
            } finally {
                cleanThread();
                CDC.clear();
            }
        }

        private synchronized void setThread() throws InterruptedException {
            if (_needInterruption) {
                throw new InterruptedException("Thread interrupted before execution");
            }
            _thread = Thread.currentThread();
        }

        private synchronized void cleanThread() {
            _thread = null;
        }

        @Override
        public synchronized void cancel() {
            if (_thread != null) {
                _thread.interrupt();
            } else {
                _needInterruption = true;
            }
        }
    }
}
