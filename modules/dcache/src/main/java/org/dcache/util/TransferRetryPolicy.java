package org.dcache.util;

/**
 * Transfer retry policy.
 */
public final class TransferRetryPolicy {

    private final int _retryCount;
    private final long _retryPeriod;
    private final long _timeout;
    private final long _moverTimeout;

    public TransferRetryPolicy(int retryCount, long retryPeriod,
                               long timeout, long moverTimeout)
    {
        _retryCount = retryCount;
        _retryPeriod = retryPeriod;
        _timeout = timeout;
        _moverTimeout = moverTimeout;
    }

    int getRetryCount() {
        return _retryCount;
    }

    long getRetryPeriod() {
        return _retryPeriod;
    }

    long getTotalTimeOut() {
        return _timeout;
    }

    long getMoverStartTimeout() {
        return _moverTimeout;
    }
}
