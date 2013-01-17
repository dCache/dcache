package org.dcache.cells;

import dmg.cells.nucleus.CellPath;
import diskCacheV111.util.CacheException;


/**
 * Abstract base class for MessageCallback implementations.
 */
public abstract class AbstractMessageCallback<T> implements MessageCallback<T>
{
    private T _reply;

    public abstract void success(T message);

    public T getReply()
    {
        return _reply;
    }

    @Override
    public void setReply(T message)
    {
        _reply = message;
    }

    @Override
    public void success()
    {
        success(getReply());
    }

    @Override
    public void noroute(CellPath path)
    {
        failure(CacheException.TIMEOUT, "No route to " + path);
    }

    @Override
    public void timeout(CellPath path)
    {
        failure(CacheException.TIMEOUT, "No reply from " + path);
    }
}
