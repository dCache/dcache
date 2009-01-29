package org.dcache.cells;

/**
 * Callback interface for asynchroneous message delivery. Similar to
 * CellMessageAnswerable, but with a more dCache specific design.
 */
public interface MessageCallback<T>
{
    void success(T message);
    void failure(int rc, Object error);
    void noroute();
    void timeout();
}