package org.dcache.pool.classic;

/**
 * An interface for object which allow to interrupt an operation in progress.
 */
public interface Cancelable {

    void cancel();
}
