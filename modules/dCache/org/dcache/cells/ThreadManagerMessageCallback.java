package org.dcache.cells;

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

    public void success(final T message)
    {
        ThreadManager.execute(new Runnable() {
                public void run()
                {
                    _inner.success(message);
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

    public void noroute()
    {
        ThreadManager.execute(new Runnable() {
                public void run()
                {
                    _inner.noroute();
                }
            });
    }

    public void timeout()
    {
        ThreadManager.execute(new Runnable() {
                public void run()
                {
                    _inner.timeout();
                }
            });
    }
}