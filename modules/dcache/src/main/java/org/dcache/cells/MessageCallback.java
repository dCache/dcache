package org.dcache.cells;

import dmg.cells.nucleus.CellPath;

/**
 * Callback interface for asynchroneous message delivery. Similar to
 * CellMessageAnswerable, but with a more dCache specific design.
 */
public interface MessageCallback<T>
{
    void setReply(T message);
    void success();
    void failure(int rc, Object error);
    void noroute(CellPath cell);
    void timeout(String error);
}
