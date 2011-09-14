package org.dcache.pool.classic;

import diskCacheV111.util.CacheException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import dmg.cells.nucleus.CDC;

/**
 *
 * @since 1.9.11
 */
public class ClassicPostExecutionService implements PostTransferExecutionService
{
    private final ExecutorService _executor = Executors.newCachedThreadPool();

    public void execute(final PoolIORequest request)
    {
        final CDC cdc = new CDC();
        _executor.execute(new Runnable() {
                @Override
                public void run()
                {
                    cdc.restore();
                    try {
                        request.close();
                    } catch (InterruptedException e) {
                        request.setTransferStatus(CacheException.DEFAULT_ERROR_CODE,
                                                  "Transfer was killed");
                    } catch (CacheException e) {
                        int rc = e.getRc();
                        String msg = e.getMessage();
                        if (rc == CacheException.ERROR_IO_DISK) {
                            request.getFaultListener().faultOccurred(new FaultEvent("repository", FaultAction.DISABLED, msg, e));
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
                    CDC.clear();
                }
            });
    }

}
