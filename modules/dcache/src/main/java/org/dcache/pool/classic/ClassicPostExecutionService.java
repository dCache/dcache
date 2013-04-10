package org.dcache.pool.classic;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;

import diskCacheV111.util.CacheException;

import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.util.CDCListeningExecutorServiceDecorator;

/**
 *
 * @since 1.9.11
 */
public class ClassicPostExecutionService implements PostTransferExecutionService
{
    private final ListeningExecutorService _executor =
            new CDCListeningExecutorServiceDecorator(
                    MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(
                            new ThreadFactoryBuilder().setNameFormat("post-execution-%d").build())));

    @Override
    public ListenableFuture<?> execute(final PoolIORequest request)
    {
        return _executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    request.close();
                } catch (InterruptedException e) {
                    request.setTransferStatus(CacheException.DEFAULT_ERROR_CODE,
                            "Transfer was killed");
                } catch (CacheException e) {
                    int rc = e.getRc();
                    String msg = e.getMessage();
                    if (rc == CacheException.ERROR_IO_DISK) {
                        request.getFaultListener()
                                .faultOccurred(new FaultEvent("repository", FaultAction.DISABLED, msg, e));
                    }
                    request.setTransferStatus(rc, msg);
                } catch (Exception e) {
                    request.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                            "Transfer failed due to unexpected exception: " + e.getMessage());
                } catch (Throwable e) {
                    Thread t = Thread.currentThread();
                    t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    request.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                            "Transfer failed due to unexpected exception: " + e.getMessage());
                }
                request.sendBillingMessage();
                request.sendFinished();
            }
        });
    }

    public void shutdown()
    {
        _executor.shutdown();
    }
}
