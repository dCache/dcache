package org.dcache.util;

public final class TransferRetryPolicy
{
    private final int _retryCount;
    private final long _retryPeriod;
    private final long _timeout;

    public TransferRetryPolicy(int retryCount, long retryPeriod, long timeout)
    {
        _retryCount = retryCount;
        _retryPeriod = retryPeriod;
        _timeout = timeout;
    }

    int getRetryCount()
    {
        return _retryCount;
    }

    long getRetryPeriod()
    {
        return _retryPeriod;
    }

    long getTotalTimeOut()
    {
        return _timeout;
    }
}
