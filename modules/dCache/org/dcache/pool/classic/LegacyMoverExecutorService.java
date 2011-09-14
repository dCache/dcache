package org.dcache.pool.classic;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
    public Future execute(final PoolIORequest request,
                          final CompletionHandler completionHandler)
    {
        final CDC cdc = new CDC();
        return _executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    cdc.restore();
                    request.getTransfer().transfer();
                    completionHandler.completed(null, null);
                } catch (Exception e) {
                    _log.error("Transfer failed: {}", e.toString());
                    completionHandler.failed(e, null);
                } catch (Throwable e) {
                    _log.error("Transfer failed:", e);
                    Thread t = Thread.currentThread();
                    t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    completionHandler.failed(e, null);
                } finally {
                    CDC.clear();
                }
            }
        });
    }
}
