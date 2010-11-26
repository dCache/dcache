package org.dcache.pool.classic;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.dcache.util.CDCThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @simce 1.9.11
 */
public class LegacyMoverExecutorService implements MoverExecutorService {

    private final static Logger _log = LoggerFactory.getLogger(LegacyMoverExecutorService.class);
    private final static String _name = LegacyMoverExecutorService.class.getSimpleName();
    private final ExecutorService _executor = Executors.newCachedThreadPool(
            new CDCThreadFactory(Executors.defaultThreadFactory()) {

                public Thread newThread(Runnable r) {
                    Thread t = super.newThread(r);
                    t.setName(_name + "-worker");
                    return t;
                }
            });

    @Override
    public Future execute(final PoolIORequest request, final CompletionHandler completionHandler) {
        return _executor.submit(new Runnable() {

            @Override
            public void run() {
                try {
                    request.getTransfer().transfer();
                    completionHandler.completed(null, null);
                } catch (Throwable e) {
                    _log.error("Transfer failed", e);
                    completionHandler.failed(e, null);
                }
            }
        });
    }
}
