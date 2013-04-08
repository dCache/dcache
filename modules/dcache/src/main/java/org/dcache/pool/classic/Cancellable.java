package org.dcache.pool.classic;

/**
 * An interface for object which allow to interrupt an operation in progress.
 */
public interface Cancellable
{
    void cancel();
}
