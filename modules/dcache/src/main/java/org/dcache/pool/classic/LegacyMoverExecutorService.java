package org.dcache.pool.classic;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

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

    private final ListeningExecutorService _executor =
            MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat(_name + "-worker-%d").build()));

    @Override
    public ListenableFuture<Void> execute(PoolIORequest request)
    {
        return _executor.submit(new MoverTask(request));
    }

    public void shutdown()
    {
        _executor.shutdown();
    }

    private static class MoverTask implements Callable<Void>
    {
        private final PoolIORequest _request;
        private final CDC _cdc = new CDC();

        public MoverTask(PoolIORequest request) {
            _request = request;
        }

        @Override
        public Void call() throws Exception {
            try (CDC ignored = _cdc.restore()) {
                _request.getTransfer().transfer();
                return null;
            } catch (RuntimeException e) {
                _log.error("Transfer failed due to a bug: {}", e);
                throw e;
            } catch (Exception e) {
                _log.error("Transfer failed: {}", e.toString());
                throw e;
            } catch (Throwable e) {
                _log.error("Transfer failed:", e);
                Thread t = Thread.currentThread();
                t.getUncaughtExceptionHandler().uncaughtException(t, e);
                throw e;
            }
        }
    }
}
