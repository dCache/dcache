package org.dcache.services.billing.db.exceptions;

/**
 * @author arossi
 */
public class BillingQueryException extends Throwable {

    private static final long serialVersionUID = -5160138147238131675L;

    public BillingQueryException() {
    }

    /**
     * @param message
     */
    public BillingQueryException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public BillingQueryException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public BillingQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
