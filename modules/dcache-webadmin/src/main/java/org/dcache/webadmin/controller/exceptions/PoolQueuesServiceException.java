package org.dcache.webadmin.controller.exceptions;

/**
 *
 * @author jans
 */
public class PoolQueuesServiceException extends Exception {

    private static final long serialVersionUID = 5826359410043206879L;

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public PoolQueuesServiceException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public PoolQueuesServiceException(Throwable cause) {
        super(cause);
    }
}
