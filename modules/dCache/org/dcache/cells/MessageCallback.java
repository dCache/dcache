package org.dcache.cells;

import diskCacheV111.vehicles.Message;

/**
 * Callback interface for asynchroneous message delivery. Similar to
 * CellMessageAnswerable, but with a more dCache specific design.
 */
public interface MessageCallback<T extends Message>
{
    void success(T message);
    void failure(int rc, Object error);
    void noroute();
    void timeout();
}