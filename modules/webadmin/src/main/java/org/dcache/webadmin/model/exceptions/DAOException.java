package org.dcache.webadmin.model.exceptions;

/**
 * used when Dataaccess goes wrong
 * @author jan schaefer 29-10-2009
 */
public class DAOException extends Exception {

    private static final long serialVersionUID = -1276108376482381265L;

    /**
     * Constructor with error message and root cause.
     *
     * @param msg
     *            the error message associated with the exception
     * @param cause
     *            the root cause of the exception
     */
    public DAOException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor with error message and root cause.
     *
     * @param cause
     *            the root cause of the exception
     */
    public DAOException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor with error message.
     *
     * @param cause
     *            the root cause of the exception
     */
    public DAOException(String msg) {
        super(msg);
    }
}
