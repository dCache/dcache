package org.dcache.webadmin.controller.exceptions;

/**
 *
 * @author jans
 */
public class PoolSelectionSetupServiceException extends Exception {

    private static final long serialVersionUID = -7022676563148843593L;

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public PoolSelectionSetupServiceException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public PoolSelectionSetupServiceException(Throwable cause) {
        super(cause);
    }
}
