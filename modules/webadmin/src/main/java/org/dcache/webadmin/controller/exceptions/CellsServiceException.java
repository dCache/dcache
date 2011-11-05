package org.dcache.webadmin.controller.exceptions;

/**
 *
 * @author jans
 */
public class CellsServiceException extends Exception {

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public CellsServiceException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public CellsServiceException(Throwable cause) {
        super(cause);
    }
}
