package org.dcache.pool.classic;

import java.util.concurrent.Future;

/**
 *
 * @since 1.9.11
 */
interface MoverExecutorService {

    Future execute(PoolIOTransfer transfer, CompletionHandler completionHandler);
}
