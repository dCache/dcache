package org.dcache.webadmin.controller.exceptions;

/**
 *
 * @author jans
 */
public class PoolGroupServiceException extends Exception {

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public PoolGroupServiceException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public PoolGroupServiceException(Throwable cause) {
        super(cause);
    }
}
