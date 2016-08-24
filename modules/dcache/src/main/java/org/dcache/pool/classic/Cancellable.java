package org.dcache.pool.classic;

import javax.annotation.Nullable;

/**
 * An interface for object which allow to interrupt an operation in progress.
 */
public interface Cancellable
{
    void cancel(@Nullable String explanation);
}
