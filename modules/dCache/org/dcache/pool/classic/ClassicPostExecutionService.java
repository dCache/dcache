package org.dcache.pool.classic;

import diskCacheV111.util.CacheException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;

/**
 *
 * @since 1.9.11
 */
public class ClassicPostExecutionService implements PostTransferExecutionService {

    private final ExecutorService _executor = Executors.newCachedThreadPool();

    public void execute(final PoolIORequest request) {

        _executor.execute( new Runnable() {

            public void run() {
                int rc;
                String msg;
                try {
                    request.close();
                    rc = 0;
                    msg = "";
                } catch (InterruptedException e) {
                    rc = CacheException.DEFAULT_ERROR_CODE;
                    msg = "Transfer was killed";
                } catch (CacheException e) {
                    rc = e.getRc();
                    msg = e.getMessage();
                    if (rc == CacheException.ERROR_IO_DISK) {
                        request.getFaultListener().faultOccurred(new FaultEvent("repository", FaultAction.DISABLED, msg, e));
                    }
                    rc = e.getRc();
                    msg = e.getMessage();
                } catch (RuntimeException e) {
                    rc = CacheException.UNEXPECTED_SYSTEM_EXCEPTION;
                    msg = "Transfer failed due to unexpected exception: " + e;
                } catch (Exception e) {
                    rc = CacheException.DEFAULT_ERROR_CODE;
                    msg = "Transfer failed: " + e.getMessage();
                }
                request.sendFinished(rc, msg);
            }
        });

    }

}
