package org.dcache.pool.classic;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.InterruptedIOException;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;

import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.util.CDCExecutorServiceDecorator;

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
            new CDCExecutorServiceDecorator(
                    Executors.newCachedThreadPool(
                            new ThreadFactoryBuilder().setNameFormat(_name + "-worker-%d").build()));
    private FaultListener _faultListener;

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

    @Required
    public void setFaultListener(FaultListener faultListener) {
        _faultListener = faultListener;
    }

    private class MoverTask implements Runnable, Cancellable
    {
        private final PoolIORequest _request;
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
                try {
                    _request.getTransfer().transfer();
                } catch (Throwable t) {
                    _completionHandler.failed(t, null);
                    throw t;
                }
                _completionHandler.completed(null, null);
            } catch (DiskErrorCacheException e) {
                _log.error("Transfer failed due to a disk error: {}", e.toString());
                _faultListener.faultOccurred(new FaultEvent("repository", FaultAction.DISABLED, e.getMessage(), e));
                _request.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
            } catch (CacheException e) {
                _log.error("Transfer failed: {}", e.getMessage());
                _request.setTransferStatus(e.getRc(), e.getMessage());
            } catch (InterruptedIOException | InterruptedException e) {
                _log.error("Transfer was forcefully killed");
                _request.setTransferStatus(CacheException.DEFAULT_ERROR_CODE, "Transfer was forcefully killed");
            } catch (RuntimeException e) {
                _log.error("Transfer failed due to a bug", e);
                _request.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
            } catch (Exception e) {
                _log.error("Transfer failed: {}", e.toString());
                _request.setTransferStatus(CacheException.DEFAULT_ERROR_CODE, e.getMessage());
            } catch (Throwable e) {
                _log.error("Transfer failed:", e);
                Thread t = Thread.currentThread();
                t.getUncaughtExceptionHandler().uncaughtException(t, e);
                _request.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
            } finally {
                cleanThread();
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
