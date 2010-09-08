package org.dcache.util;

/**
 * Short hand predefined {@link TransferRetryPolicy}
 */
public final class TransferRetryPolicies {

    /* no instances allowed */
    private TransferRetryPolicies() {}

    /**
     * Create a {@link TransferRetryPolicy} which will let {@link Transfer}
     * to try proceed with request on any recoverable errors ( like timeouts or
     * pool does not contains the file).
     *
     * @return policy
     */
    public static TransferRetryPolicy neverFailPolicy() {
        return new TransferRetryPolicy(Integer.MAX_VALUE, Long.MAX_VALUE, 500);
    }

    /**
     * Create a {@link TransferRetryPolicy} this which will let {@link Transfer}
     * to start a transfer with in given timeout. No retries performed in case of
     * errors.
     * @param timeout
     * @return policy
     */
    public static TransferRetryPolicy tryOncePolicy(long timeout) {
        return new TransferRetryPolicy(1, timeout, 500);
    }

    /**
     * Create a {@link TransferRetryPolicy} this which will let {@link Transfer}
     * to start a transfer with in given timeout. Request will be retried on
     * any recoverable  error.
     *
     * @param timeout
     * @return policy
     */
    public static TransferRetryPolicy tryTillTimeout(long timeout) {
        return new TransferRetryPolicy(Integer.MAX_VALUE, timeout, 500);
    }

}
