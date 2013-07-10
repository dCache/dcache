package org.dcache.webadmin.controller.exceptions;

/**
 *
 * @author jan schaefer 29-10-2009
 */
public class PoolSpaceServiceException extends Exception {

    private static final long serialVersionUID = 5728158781746628381L;

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public PoolSpaceServiceException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public PoolSpaceServiceException(Throwable cause) {
        super(cause);
    }
}
