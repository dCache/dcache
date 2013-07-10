package org.dcache.webadmin.controller.exceptions;

public class LogInServiceException extends Exception {

    private static final long serialVersionUID = -157172026921099223L;

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public LogInServiceException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     */
    public LogInServiceException(String msg) {
        super(msg);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public LogInServiceException(Throwable cause) {
        super(cause);
    }
}
