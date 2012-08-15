package org.dcache.webadmin.controller.exceptions;

/**
 *
 * @author jans
 */
public class TapeTransfersServiceException extends Exception{

    private static final long serialVersionUID = -2881765431142537763L;

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public TapeTransfersServiceException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public TapeTransfersServiceException(Throwable cause) {
        super(cause);
    }
}
