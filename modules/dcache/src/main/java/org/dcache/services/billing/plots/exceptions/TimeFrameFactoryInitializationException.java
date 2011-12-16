package org.dcache.services.billing.plots.exceptions;

/**
 * @author arossi
 */
public class TimeFrameFactoryInitializationException extends Throwable {

    private static final long serialVersionUID = -5160138147238131675L;

    public TimeFrameFactoryInitializationException() {
    }

    /**
     * @param message
     */
    public TimeFrameFactoryInitializationException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public TimeFrameFactoryInitializationException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public TimeFrameFactoryInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
