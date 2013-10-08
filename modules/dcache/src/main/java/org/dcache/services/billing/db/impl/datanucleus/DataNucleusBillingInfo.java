package org.dcache.services.billing.db.impl.datanucleus;

import org.datanucleus.FetchPlan;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.IPlotData;
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

    public static final String DEFAULT_PROPERTIES
        = "org/dcache/services/billing/db/datanucleus.properties";

    private PersistenceManagerFactory pmf;

    /*
     * Initializes DataNucleus factory and in-memory object caching.
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
        } catch (Exception t) {
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

    @Override
    public void commit(Collection<IPlotData> data)
                    throws BillingStorageException {
        PersistenceManager insertManager = pmf.getPersistenceManager();
        Transaction tx = insertManager.currentTransaction();
        try {
            tx.begin();
            insertManager.makePersistentAll(data);
            tx.commit();
        } catch (JDOUserException t) {
            printSQLException("committing  " + data.size() + " cached objects",
                            t);
            throw new BillingStorageException(t.getMessage(), t.getCause());
        } finally {
            try {
                rollbackIfActive(tx);
            } finally {
                /*
                 * closing is necessary in order to avoid memory leaks
                 */
                insertManager.close();
            }
        }
    }

    @Override
    public <T> Collection<T> get(Class<T> type, String filter,
                    String parameters, Object... values)
                                    throws BillingQueryException {
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
            return detached;
        } catch (JDOUserException t) {
            String message = "get: " + type + ", " + filter + ", " + parameters
                            + ", " + Arrays.asList(values);
            printSQLException(message, t);
            throw new BillingQueryException(message, t);
        } finally {
            try {
                rollbackIfActive(tx);
            } finally {
                /*
                 * closing is necessary in order to avoid memory leak in the
                 * persistence manager factory
                 */
                readManager.close();
            }
        }
    }

    @Override
    public <T> Collection<T> get(Class<T> type) throws BillingQueryException {
        return get(type, null, null, (Object) null);
    }

    @Override
    public <T> Collection<T> get(Class<T> type, String filter, Object... values)
                    throws BillingQueryException {
        return get(type, filter, null, values);
    }

    @Override
    public void close() {
        super.close();
        if (pmf != null) {
            pmf.close();
        }
    }

    @Override
    public <T> long remove(Class<T> type) throws BillingQueryException {
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
        } catch (JDOUserException t) {
            String message = "remove all instances of " + type;
            printSQLException(message, t);
            throw new BillingQueryException(message, t);
        } finally {
            try {
                rollbackIfActive(tx);
            } finally {
                /*
                 * closing is necessary in order to avoid memory leak in the
                 * persistence manager factory
                 */
                deleteManager.close();
            }
        }
    }

    /*
     * NB: This form of delete will only work if identity-type is NOT
     * "nondurable". Currently unused. (non-Javadoc)
     */
    @Override
    public <T> long remove(Class<T> type, String filter, Object... values)
                    throws BillingQueryException {
        return remove(type, filter, null, values);
    }

    /*
     * NB: This form of delete will only work if identity-type is NOT
     * "nondurable". Currently unused. (non-Javadoc)
     */
    @Override
    public <T> long remove(Class<T> type, String filter, String parameters,
                    Object... values) throws BillingQueryException {
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
        } catch (JDOUserException t) {
            String message = "remove: " + type + ", " + filter + ", "
                            + parameters + ", " + Arrays.asList(values);
            printSQLException(message, t);
            throw new BillingQueryException(message, t);
        } finally {
            try {
                rollbackIfActive(tx);
            } finally {
                /*
                 * closing is necessary in order to avoid memory leak in the
                 * persistence manager factory
                 */
                deleteManager.close();
            }
        }
    }

    private void printSQLException(String message, Throwable t) {
        if (message != null) {
            logger.trace(message);
        }
        if (t == null)
            return;
        if (t instanceof SQLException) {
            SQLException e = (SQLException) t;
            logger.trace("Error code: {}", e.getErrorCode());
            logger.trace("SQL state: {}", e.getSQLState());
        }
        printSQLException(t.getMessage(), t.getCause());
    }

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

    private static void rollbackIfActive(Transaction tx) {
        if (tx != null && tx.isActive()) {
            tx.rollback();
        }
    }
}
