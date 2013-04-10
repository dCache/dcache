package org.dcache.pool.classic;

import com.google.common.util.concurrent.ListenableFuture;

/**
 *
 * @since 1.9.11
 */
public interface MoverExecutorService {
    ListenableFuture<Void> execute(PoolIORequest transfer);
}
