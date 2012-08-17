package org.dcache.services.billing.db.impl.datanucleus;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.datanucleus.FetchPlan;
import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.exceptions.BillingInitializationException;
import org.dcache.services.billing.db.exceptions.BillingQueryException;
import org.dcache.services.billing.db.exceptions.BillingStorageException;
import org.dcache.services.billing.db.impl.BaseBillingInfoAccess;

/**
 * Implements {@link IBillingInfoAccess} using <href
 * a="http://www.datanucleus.org">DataNucleus</a>.
 *
 * @see BaseBillingInfoAccess
 * @author arossi
 */
public class DataNucleusBillingInfo extends BaseBillingInfoAccess {

    public static final String DEFAULT_PROPERTIES = "org/dcache/services/billing/db/datanucleus.properties";

    private PersistenceManagerFactory pmf;
    private PersistenceManager insertManager;

    /*
     * (non-Javadoc)
     *
     * Initializes DataNucleus factory and in-memory object caching.
     *
     * @see
     * org.dcache.billing.AbstractBillingInfoAccess#initialize(java.lang.String)
     */
    @Override
    public void initializeInternal() throws BillingInitializationException {
        addJdbcDNProperties();
        try {
            if (propertiesPath != null && !"".equals(propertiesPath.trim())) {
                File file = new File(propertiesPath);
                if (!file.exists()) {
                    throw new FileNotFoundException(
                                    "Cannot run BillingInfoCell for properties file: "
                                                    + file);
                }
                properties.load(new FileInputStream(file));
            } else {
                ClassLoader classLoader = Thread.currentThread()
                                .getContextClassLoader();
                URL resource = classLoader.getResource(DEFAULT_PROPERTIES);
                if (resource == null) {
                    throw new FileNotFoundException(
                                    "Cannot run BillingInfoCell for properties resource: "
                                                    + resource);
                }
                properties.load(resource.openStream());
            }
            pmf = JDOHelper.getPersistenceManagerFactory(properties);
        } catch (Throwable t) {
            throw new BillingInitializationException(t);
        }
    }

    /**
     * From injection, but will be overwritten by any properties set in the
     * .properties resource or file.
     */
    private void addJdbcDNProperties() {
        if (jdbcDriver != null && !"".equals(jdbcDriver))
            properties.setProperty("datanucleus.ConnectionDriverName",
                            jdbcDriver);
        if (jdbcUrl != null && !"".equals(jdbcUrl))
            properties.setProperty("datanucleus.ConnectionURL", jdbcUrl);
        if (jdbcUser != null && !"".equals(jdbcUser))
            properties.setProperty("datanucleus.ConnectionUserName", jdbcUser);
        if (jdbcPassword != null && !"".equals(jdbcPassword))
            properties.setProperty("datanucleus.ConnectionPassword",
                            jdbcPassword);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.services.billing.db.IBillingInfoAccess#put(java.lang.Object)
     */
    @Override
    public <T> void put(T data) throws BillingStorageException {
        if (!isRunning())
            return;
        synchronized (this) {
            Transaction tx = null;
            try {
                if (insertManager == null) {
                    logger.debug("put, new write manager ...");
                    insertManager = pmf.getPersistenceManager();
                }
                tx = insertManager.currentTransaction();
                if (!tx.isActive()) {
                    tx.begin();
                }
                insertManager.makePersistent(data);
                insertCount++;
            } catch (Throwable e) {
                printSQLException("put " + data, e);
                rollbackIfActive(tx);
            }
        }
        doCommitIfNeeded(false);
    }

    /*
     * (non-Javadoc) Runs DataNucleus transaction if threshold is reached or
     * force is true.
     *
     * @see
     * org.dcache.billing.impl.AbstractBillingInfoAccess#doCommitIfNeeded(boolean
     * )
     */
    protected void doCommitIfNeeded(boolean force) {
        synchronized (this) {
            logger.debug("doCommitIfNeeded, count={}", insertCount);
            if (force || insertCount >= maxInsertsBeforeCommit) {
                Transaction tx = null;
                try {
                    if (insertManager != null) {
                        logger.debug("committing {} cached objects",
                                        insertCount);
                        tx = insertManager.currentTransaction();
                        tx.commit();
                    }
                } catch (Throwable t) {
                    printSQLException("committing  " + insertCount
                                    + " cached objects", t);
                    rollbackIfActive(tx);
                } finally {
                    /*
                     * closing is necessary in order to avoid memory leaks
                     */
                    if (insertManager != null) {
                        insertManager.close();
                        insertManager = null;
                    }
                    insertCount = 0;
                }
            }
        }
    }

    /*
     * Returns detached copy of query result. (non-Javadoc)
     *
     * @see
     * org.dcache.services.billing.db.IBillingInfoAccess#get(java.lang.Class,
     * java.lang.String, java.lang.String, java.lang.Object[])
     */
    @Override
    public <T> Collection<T> get(Class<T> type, String filter,
                    String parameters, Object... values)
                                    throws BillingQueryException {
        if (!isRunning())
            return null;
        PersistenceManager readManager = pmf.getPersistenceManager();
        Transaction tx = readManager.currentTransaction();
        try {
            tx.begin();
            Query query = createQuery(readManager, type, filter, parameters);
            logger.debug("created query {}", query);
            Collection<T> c = (values == null ? ((Collection<T>) query
                            .execute()) : ((Collection<T>) query
                                            .executeWithArray(values)));
            logger.debug("got collection {}", c);
            Collection<T> detached = readManager.detachCopyAll(c);
            logger.debug("got detatched collection {}", detached);
            tx.commit();
            logger.debug("successfully executed {}",
                            "get: {}, {}. {}. {}",
                            new Object[] { type, filter, parameters,
                            Arrays.asList(values) });
            return detached;
        } catch (Throwable t) {
            String message = "get: " + type + ", " + filter + ", " + parameters
                            + ", " + Arrays.asList(values);
            printSQLException(message, t);
            throw new BillingQueryException(message, t);
        } finally {
            rollbackIfActive(tx);
            /*
             * closing is necessary in order to avoid memory leak in the
             * persistence manager factory
             */
            readManager.close();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.IBillingInfoAccess#get(java.lang.Class)
     */
    @Override
    public <T> Collection<T> get(Class<T> type) throws BillingQueryException {
        return get(type, null, null, (Object) null);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.IBillingInfoAccess#get(java.lang.Class,
     * java.lang.String, java.lang.Object[])
     */
    @Override
    public <T> Collection<T> get(Class<T> type, String filter, Object... values)
                    throws BillingQueryException {
        return get(type, filter, null, values);
    }

    /*
     * Close the daemon (super), then shut down the thread and close the
     * manager. (non-Javadoc)
     *
     * @see org.dcache.billing.AbstractBillingInfoAccess#close()
     */
    @Override
    public void close() {
        super.close();
        if (pmf != null) {
            synchronized (this) {
                if (insertManager != null) {
                    insertManager.close();
                }
            }
            pmf.close();
        }
    }

    /*
     * Does bulk deletes. Properties file must set
     * <code>datanucleus.query.jdoql.allowAll=true</code> (non-Javadoc)
     *
     * @see org.dcache.billing.IBillingInfoAccess#removeAll(java.lang.Class)
     */
    @Override
    public <T> long remove(Class<T> type) throws BillingQueryException {
        if (!isRunning())
            return 0;
        PersistenceManager deleteManager = pmf.getPersistenceManager();
        Transaction tx = deleteManager.currentTransaction();
        logger.debug("remove all instances of {}", type);
        long removed = 0;
        try {
            tx.begin();
            Query query = deleteManager.newQuery("DELETE FROM "
                            + type.getName());
            removed = (Long) query.execute();
            tx.commit();
            logger.debug("successfully removed " + removed
                            + " entries of type " + type);
            return removed;
        } catch (Throwable t) {
            String message = "remove all instances of " + type;
            printSQLException(message, t);
            throw new BillingQueryException(message, t);
        } finally {
            rollbackIfActive(tx);
            /*
             * closing is necessary in order to avoid memory leak in the
             * persistence manager factory
             */
            deleteManager.close();
        }
    }

    /*
     * NB: This form of delete will only work if identity-type is NOT
     * "nondurable". Currently unused. (non-Javadoc)
     *
     * @see org.dcache.billing.IBillingInfoAccess#remove(java.lang.Class,
     * java.lang.String, java.lang.Object[])
     */
    @Override
    public <T> long remove(Class<T> type, String filter, Object... values)
                    throws BillingQueryException {
        return remove(type, filter, null, values);
    }

    /*
     * NB: This form of delete will only work if identity-type is NOT
     * "nondurable". Currently unused. (non-Javadoc)
     *
     * @see org.dcache.billing.IBillingInfoAccess#remove(java.lang.Class,
     * java.lang.String, java.lang.String, java.lang.Object[])
     */
    @Override
    public <T> long remove(Class<T> type, String filter, String parameters,
                    Object... values) throws BillingQueryException {
        if (!isRunning())
            return 0;
        PersistenceManager deleteManager = pmf.getPersistenceManager();
        Transaction tx = deleteManager.currentTransaction();
        long removed = 0;
        try {
            tx.begin();
            Query query = createQuery(deleteManager, type, filter, parameters);
            if (values == null)
                removed = query.deletePersistentAll();
            else
                removed = query.deletePersistentAll(values);
            tx.commit();
            logger.debug("successfully removed {} entries of type {}", removed,
                            type);
            return removed;
        } catch (Throwable t) {
            String message = "remove: " + type + ", " + filter + ", "
                            + parameters + ", " + Arrays.asList(values);
            printSQLException(message, t);
            throw new BillingQueryException(message, t);
        } finally {
            rollbackIfActive(tx);
            /*
             * closing is necessary in order to avoid memory leak in the
             * persistence manager factory
             */
            deleteManager.close();
        }
    }

    /**
     * Tries to extract embedded messages from SQL exceptions.
     *
     * @param message
     * @param t
     */
    private void printSQLException(String message, Throwable t) {
        if (t == null)
            return;
        if (t instanceof SQLException) {
            SQLException e = (SQLException) t;
            logger.error(e.getMessage());
            logger.error("Error code: {}", e.getErrorCode());
            logger.error("SQL state: {}", e.getSQLState());
        } else {
            logger.error(message, t);
        }
        printSQLException(message, t.getCause());
    }

    /**
     * Convenience method.
     *
     * @param pm
     * @param type
     * @param filter
     * @param parameters
     * @return appropriate Query
     */
    private static Query createQuery(PersistenceManager pm, Class type,
                    String filter, String parameters) {
        if (parameters == null) {
            if (filter == null)
                return pm.newQuery(type);
            return pm.newQuery(type, filter);
        }
        Query query = pm.newQuery(type);
        query.setFilter(filter);
        query.declareParameters(parameters);
        query.addExtension("datanucleus.rdbms.query.resultSetType",
                        "scroll-insensitive");
        query.addExtension("datanucleus.query.resultCacheType", "none");
        query.getFetchPlan().setFetchSize(FetchPlan.FETCH_SIZE_OPTIMAL);
        return query;
    }

    /**
     * Convenience method.
     *
     * @param tx
     *            transaction
     */
    private static void rollbackIfActive(Transaction tx) {
        if (tx != null && tx.isActive()) {
            tx.rollback();
        }
    }

    /**
     * @param propetiesPath
     *            the propetiesPath to set
     */
    public void setPropertiesPath(String propetiesPath) {
        this.propertiesPath = propetiesPath;
    }
}
