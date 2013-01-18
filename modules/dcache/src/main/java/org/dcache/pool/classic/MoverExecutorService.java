package org.dcache.pool.classic;

import java.nio.channels.CompletionHandler;

/**
 *
 * @since 1.9.11
 */
public interface MoverExecutorService {

    Cancelable execute(PoolIORequest transfer, CompletionHandler completionHandler);
}
