package org.dcache.webadmin.model.exceptions;

/**
 * thrown when parsing goes wrong
 * @author jans
 */
public class ParsingException extends Exception {

    private static final long serialVersionUID = -5001603428492086222L;

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public ParsingException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public ParsingException(Throwable cause) {
        super(cause);
    }
}
