package org.dcache.cells;

import dmg.cells.nucleus.CellPath;
import diskCacheV111.util.ThreadManager;

/**
 * Wraps another MessageCallback. Each call is delegated to the
 * wrapped callback, however the callback is executed through the
 * ThreadManager singleton.
 */
public class ThreadManagerMessageCallback<T>
    implements MessageCallback<T>
{
    private final MessageCallback<T> _inner;

    public ThreadManagerMessageCallback(MessageCallback<T> inner)
    {
        _inner = inner;
    }

    public void setReply(T message)
    {
        _inner.setReply(message);
    }

    public void success()
    {
        ThreadManager.execute(new Runnable() {
                public void run()
                {
                    _inner.success();
                }
            });
    }

    public void failure(final int rc, final Object error)
    {
        ThreadManager.execute(new Runnable() {
                public void run()
                {
                    _inner.failure(rc, error);
                }
            });
    }

    public void noroute(final CellPath path)
    {
        ThreadManager.execute(new Runnable() {
                public void run()
                {
                    _inner.noroute(path);
                }
            });
    }

    public void timeout(final CellPath path)
    {
        ThreadManager.execute(new Runnable() {
                public void run()
                {
                    _inner.timeout(path);
                }
            });
    }
}