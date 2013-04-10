package org.dcache.pool.classic;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * A PostTransferExecutionService is invoked after a file was transferred
 * through a MoverExecutionService.
 *
 * @since 1.9.11
 */
public interface PostTransferExecutionService {
    /**
     * Submits a transfer request for processing by this post transfer execution service and
     * returns a ListenableFuture representing that task. The ListenableFuture's get method
     * will return null upon successful completion.
     *
     * @param request transfer request to submit for post processing
     * @return a ListenableFuture representing pending completion of the task
     */
    ListenableFuture<?> execute(PoolIORequest request);
}
