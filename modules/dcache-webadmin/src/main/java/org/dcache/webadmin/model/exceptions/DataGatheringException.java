package org.dcache.webadmin.model.exceptions;

/**
 * thrown when data gathering goes wrong
 * @author jans
 */
public class DataGatheringException extends Exception{

    private static final long serialVersionUID = -5837724016060125673L;

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public DataGatheringException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with  and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public DataGatheringException(String msg) {
        super(msg);
    }

    /**
     * Constructor with root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public DataGatheringException(Throwable cause) {
        super(cause);
    }
}
