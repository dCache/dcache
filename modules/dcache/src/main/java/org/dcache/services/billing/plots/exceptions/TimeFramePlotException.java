package org.dcache.services.billing.plots.exceptions;

/**
 * @author arossi
 */
public class TimeFramePlotException extends Exception {

    private static final long serialVersionUID = -5160135147238131675L;

    public TimeFramePlotException() {
    }

    /**
     * @param message
     */
    public TimeFramePlotException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public TimeFramePlotException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public TimeFramePlotException(String message, Throwable cause) {
        super(message, cause);
    }
}
