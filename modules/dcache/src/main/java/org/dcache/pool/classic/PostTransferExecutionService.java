package org.dcache.pool.classic;

/**
 *
 * @since 1.9.11
 */
public interface PostTransferExecutionService {
    void execute(PoolIORequest request);
}
