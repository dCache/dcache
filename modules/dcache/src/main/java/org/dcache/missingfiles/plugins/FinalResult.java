package org.dcache.missingfiles.plugins;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *  A handy class for returning a result that has been computed.
 */
public class FinalResult implements Future<Result>
{
    private final Result _result;

    public FinalResult(Result result)
    {
        _result = result;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return false;
    }

    @Override
    public boolean isCancelled()
    {
        return false;
    }

    @Override
    public boolean isDone()
    {
        return true;
    }

    @Override
    public Result get() throws InterruptedException, ExecutionException
    {
        return _result;
    }

    @Override
    public Result get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        return _result;
    }

}
