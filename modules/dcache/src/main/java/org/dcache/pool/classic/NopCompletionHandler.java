package org.dcache.pool.classic;

import java.nio.channels.CompletionHandler;

public class NopCompletionHandler<V, A> implements CompletionHandler<V, A>
{
    @Override
    public void completed(V result, A attachment)
    {
    }

    @Override
    public void failed(Throwable exc, A attachment)
    {
    }
}
