package org.dcache.webadmin.controller.exceptions;

/**
 *
 * @author jans
 */
public class ActiveTransfersServiceException extends Exception {

    private static final long serialVersionUID = 1104331893937852680L;

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public ActiveTransfersServiceException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public ActiveTransfersServiceException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor with error message.
     *
     * @param msg
     *            the root cause of the exception
     */
    public ActiveTransfersServiceException(String msg) {
        super(msg);
    }
}
