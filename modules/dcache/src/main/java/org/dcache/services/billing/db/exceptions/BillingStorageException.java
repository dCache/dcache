package org.dcache.services.billing.db.exceptions;

/**
 * @author arossi
 */
public class BillingStorageException extends Throwable {

    private static final long serialVersionUID = 6260065498311335542L;

    public BillingStorageException() {
    }

    /**
     * @param arg0
     */
    public BillingStorageException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public BillingStorageException(Throwable arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     * @param arg1
     */
    public BillingStorageException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

}
