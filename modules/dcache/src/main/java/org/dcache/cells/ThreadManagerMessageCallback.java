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

    @Override
    public void setReply(T message)
    {
        _inner.setReply(message);
    }

    @Override
    public void success()
    {
        ThreadManager.execute(new Runnable() {
                @Override
                public void run()
                {
                    _inner.success();
                }
            });
    }

    @Override
    public void failure(final int rc, final Object error)
    {
        ThreadManager.execute(new Runnable() {
                @Override
                public void run()
                {
                    _inner.failure(rc, error);
                }
            });
    }

    @Override
    public void noroute(final CellPath path)
    {
        ThreadManager.execute(new Runnable() {
                @Override
                public void run()
                {
                    _inner.noroute(path);
                }
            });
    }

    @Override
    public void timeout(final CellPath path)
    {
        ThreadManager.execute(new Runnable() {
                @Override
                public void run()
                {
                    _inner.timeout(path);
                }
            });
    }
}