package org.dcache.pool.classic;

/**
 *
 * @author tigran
 * @since 1.9.11
 */
public enum IoRequestState {
    CREATED,
    QUEUED,
    RUNNING,
    DONE,
    CANCELED
}
