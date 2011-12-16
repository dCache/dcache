package org.dcache.services.billing.db.exceptions;

/**
 * @author arossi
 */
public class BillingInitializationException extends Throwable {

    private static final long serialVersionUID = -5160138147230131675L;

    public BillingInitializationException() {
    }

    /**
     * @param message
     */
    public BillingInitializationException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public BillingInitializationException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public BillingInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
