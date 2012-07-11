package org.dcache.services.billing.db;

import java.util.Collection;

import org.dcache.services.billing.db.exceptions.BillingInitializationException;
import org.dcache.services.billing.db.exceptions.BillingQueryException;
import org.dcache.services.billing.db.exceptions.BillingStorageException;

/**
 * Defines DAO API for interacting with billing information.
 *
 * @author arossi
 */
public interface IBillingInfoAccess {

    void initialize() throws BillingInitializationException;

    void close();

    /**
     * @param data
     *            mapped type to be stored
     */
    <T> void put(T data);

    /**
     * @param type
     *            class of object to be retrieved
     * @return all existing objects of this type
     * @throws BillingQueryException
     */
    <T> Collection<T> get(Class<T> type) throws BillingQueryException;

    /**
     * @param type
     *            class of object to be retrieved
     * @param filter
     *            JDOQL
     * @param values
     *            to bind to filter
     * @return all matching objects of this type
     * @throws BillingQueryException
     */
    <T> Collection<T> get(Class<T> type, String filter, Object... values)
                    throws BillingQueryException;

    /**
     * @param type
     *            class of object to be retrieved
     * @param filter
     *            JDOQL
     * @param parameters
     * @param values
     *            to bind to filter
     * @return all matching objects of this type
     * @throws BillingQueryException
     */
    <T> Collection<T> get(Class<T> type, String filter, String parameters,
                    Object... values) throws BillingQueryException;

    /**
     * @param type
     *            class of object to be deleted
     * @return number of objects deleted
     * @throws BillingQueryException
     */
    <T> long remove(Class<T> type) throws BillingQueryException;

    /**
     * @param type
     *            class of object to be deleted
     * @param filter
     *            JDOQL
     * @param values
     *            to bind to filter
     * @return number of objects deleted
     * @throws BillingQueryException
     */
    <T> long remove(Class<T> type, String filter, Object... values)
                    throws BillingQueryException;

    /**
     * @param type
     *            class of object to be deleted
     * @param filter
     *            JDOQL
     * @param parameters
     * @param values
     *            to bind to filter
     * @return number of objects deleted
     * @throws BillingQueryException
     */
    <T> long remove(Class<T> type, String filter, String parameters,
                    Object... values) throws BillingQueryException;
}