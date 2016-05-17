package org.dcache.pool.classic;

/**
 *
 * @author tigran
 * @since 1.9.11
 */
public enum IoRequestState {
    NEW,
    QUEUED,
    RUNNING,
    DONE,
    CANCELED
}
