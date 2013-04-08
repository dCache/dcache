package org.dcache.pool.classic;

import java.nio.channels.CompletionHandler;

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
     * @param transfer transfer request to submit for post processing
     * @param completionHandler completion is signalled to completionHandler
     * @return a ListenableFuture representing pending completion of the task
     */
    void execute(PoolIOTransfer transfer, CompletionHandler<Void,Void> completionHandler);
}
