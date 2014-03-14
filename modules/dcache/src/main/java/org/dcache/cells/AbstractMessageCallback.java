package org.dcache.cells;

import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellPath;


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
    public void timeout(String message)
    {
        failure(CacheException.TIMEOUT, message);
    }
}
