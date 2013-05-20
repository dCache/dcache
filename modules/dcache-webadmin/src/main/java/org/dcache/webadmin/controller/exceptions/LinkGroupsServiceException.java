package org.dcache.webadmin.controller.exceptions;

/**
 *
 * @author jans
 */
public class LinkGroupsServiceException extends Exception {

    private static final long serialVersionUID = 1471502551248141119L;

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public LinkGroupsServiceException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public LinkGroupsServiceException(Throwable cause) {
        super(cause);
    }
}
