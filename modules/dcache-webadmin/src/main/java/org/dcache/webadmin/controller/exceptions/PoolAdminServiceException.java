package org.dcache.webadmin.controller.exceptions;

/**
 *
 * @author jans
 */
public class PoolAdminServiceException extends Exception {

    private static final long serialVersionUID = 3533010074031955226L;

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public PoolAdminServiceException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public PoolAdminServiceException(Throwable cause) {
        super(cause);
    }
}
