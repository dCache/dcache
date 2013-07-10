package org.dcache.util;

import java.util.concurrent.TimeUnit;

/**
 * Short hand predefined {@link TransferRetryPolicy}
 */
public final class TransferRetryPolicies
{
    public final static long RETRY_PERIOD = TimeUnit.SECONDS.toMillis(30);
    public final static long MOVER_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    /* no instances allowed */
    private TransferRetryPolicies() {}

    /**
     * Create a {@link TransferRetryPolicy} which will let {@link
     * Transfer} to try proceed with request on any recoverable errors
     * ( like timeouts or pool does not contains the file).
     *
     * @return policy
     */
    public static TransferRetryPolicy neverFailPolicy() {
        return new TransferRetryPolicy(Integer.MAX_VALUE, RETRY_PERIOD,
                                       Long.MAX_VALUE, MOVER_TIMEOUT);
    }

    /**
     * Create a {@link TransferRetryPolicy} this which will let {@link
     * Transfer} to start a transfer with in given timeout. No retries
     * performed in case of errors.
     *
     * @param millis
     * @return policy
     */
    public static TransferRetryPolicy tryOncePolicy(long millis) {
        return new TransferRetryPolicy(1, 0, millis, MOVER_TIMEOUT);
    }

    public static TransferRetryPolicy tryOncePolicy(long timeout, TimeUnit unit) {
        return tryOncePolicy(unit.toMillis(timeout));
    }

    /**
     * Create a {@link TransferRetryPolicy} this which will let {@link
     * Transfer} to start a transfer with in given timeout. Request
     * will be retried on any recoverable error.
     *
     * @param millis
     * @return policy
     */
    public static TransferRetryPolicy tryTillTimeout(long millis) {
        return new TransferRetryPolicy(Integer.MAX_VALUE, RETRY_PERIOD,
                                       millis, MOVER_TIMEOUT);
    }

    public static TransferRetryPolicy tryTillTimeout(long timeout, TimeUnit unit) {
        return tryTillTimeout(unit.toMillis(timeout));
    }
}
