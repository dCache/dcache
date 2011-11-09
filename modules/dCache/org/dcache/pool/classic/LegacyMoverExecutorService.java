package org.dcache.pool.classic;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dmg.cells.nucleus.CDC;

/**
 *
 * @simce 1.9.11
 */
public class LegacyMoverExecutorService implements MoverExecutorService
{
    private final static Logger _log =
        LoggerFactory.getLogger(LegacyMoverExecutorService.class);
    private final static String _name =
        LegacyMoverExecutorService.class.getSimpleName();

    private final ExecutorService _executor =
        Executors.newCachedThreadPool(
            new ThreadFactory()
            {
                private int _counter = 0;

                private ThreadFactory _factory =
                    Executors.defaultThreadFactory();

                public Thread newThread(Runnable r) {
                    Thread t = _factory.newThread(r);
                    t.setName(_name + "-worker-" + ++_counter);
                    return t;
                }
            });

    @Override
    public Cancelable execute(final PoolIORequest request,
                          final CompletionHandler completionHandler)
    {
        final MoverTask moverTask = new MoverTask(request, completionHandler);
        _executor.execute(moverTask);
        return moverTask;
    }

    private static class MoverTask implements Runnable, Cancelable {

        private final PoolIORequest _request;
        private final CDC _cdc = new CDC();
        private final CompletionHandler _completionHandler;

        private Thread _thread;
        private boolean _needInterruption = false;

        public MoverTask(PoolIORequest request, CompletionHandler completionHandler) {
            _request = request;
            _completionHandler = completionHandler;
        }

        @Override
        public void run() {

            try {
                setThread();
                _cdc.restore();
                _request.getTransfer().transfer();
                _completionHandler.completed(null, null);
            } catch (Exception e) {
                _log.error("Transfer failed: {}", e.toString());
                _completionHandler.failed(e, null);
            } catch (Throwable e) {
                _log.error("Transfer failed:", e);
                Thread t = Thread.currentThread();
                t.getUncaughtExceptionHandler().uncaughtException(t, e);
                _completionHandler.failed(e, null);
            } finally {
                cleanThread();
                CDC.clear();
            }

        }

        private synchronized void setThread() throws InterruptedException {
            if(_needInterruption)
                throw new InterruptedException("Thread interrupted before excecution");

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
