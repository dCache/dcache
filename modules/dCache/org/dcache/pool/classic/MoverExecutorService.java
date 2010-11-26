package org.dcache.pool.classic;

import java.util.concurrent.Future;

/**
 *
 * @since 1.9.11
 */
public interface MoverExecutorService {

    Future execute(PoolIORequest transfer, CompletionHandler completionHandler);
}
