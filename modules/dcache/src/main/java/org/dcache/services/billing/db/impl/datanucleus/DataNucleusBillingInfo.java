/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.services.billing.db.impl.datanucleus;

import com.google.common.base.Strings;
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
import java.io.InputStream;
import java.net.URL;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.exceptions.BillingInitializationException;
import org.dcache.services.billing.db.exceptions.BillingQueryException;
import org.dcache.services.billing.db.impl.BaseBillingInfoAccess;
import org.dcache.services.billing.histograms.data.IHistogramData;

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

    private static Query createQuery(PersistenceManager pm, Class<?> type,
                    String filter, String parameters) {
        Query query = pm.newQuery(type);

        if (filter != null) {
            query.setFilter(filter);
        }

        if (parameters != null) {
            query.declareParameters(parameters);
        }

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

    @Override
    public void commit(Collection<IHistogramData> data)
                    throws BillingQueryException {
        PersistenceManager insertManager = pmf.getPersistenceManager();
        Transaction tx = insertManager.currentTransaction();
        try {
            tx.begin();
            insertManager.makePersistentAll(data);
            tx.commit();
        } catch (JDOUserException t) {
            printSQLException("committing  " + data.size() + " cached objects",
                            t);
            throw new BillingQueryException(t.getMessage(), t.getCause());
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
    public <T> Collection<T> get(Class<T> type) throws BillingQueryException {
        return get(type, null, null, (Object) null);
    }

    @Override
    public <T> Collection<T> get(Class<T> type, String filter, Object... values)
                    throws BillingQueryException {
        return get(type, filter, null, values);
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
            logger.trace("created query {}", query);
            Collection<T> c = (values == null ? ((Collection<T>) query.execute())
                            : ((Collection<T>) query.executeWithArray(values)));
            logger.trace("collection size = {}", c.size());
            Collection<T> detached = readManager.detachCopyAll(c);
            logger.trace("got detatched collection {}", detached);
            tx.commit();
            logger.trace("successfully executed {}",
                            "get: {}, {}. {}. {}",
                            type, filter, parameters,
                            values == null ? null : Arrays.asList(values) );
            return detached;
        } catch (JDOUserException t) {
            String message = "get: " + type + ", " + filter + ", " + parameters
                            + ", " + Arrays.asList(values);
            printSQLException(message, t);
            throw new BillingQueryException(t);
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
                try (InputStream stream = new FileInputStream(file)) {
                    properties.load(stream);
                }

            } else {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                URL resource = classLoader.getResource(DEFAULT_PROPERTIES);
                if (resource == null) {
                    throw new FileNotFoundException(
                                    "Cannot run BillingInfoCell"
                                                    + "; cannot find resource "
                                                    + DEFAULT_PROPERTIES);
                }
                properties.load(resource.openStream());
            }
            pmf = JDOHelper.getPersistenceManagerFactory(properties);
        } catch (Exception t) {
            throw new BillingInitializationException(t);
        }
    }


    /**
     * Does bulk deletes. Properties file must set
     * <code>datanucleus.query.jdoql.allowAll=true</code> (non-Javadoc)
     */
    @Override
    public <T> long remove(Class<T> type) throws BillingQueryException {
        PersistenceManager deleteManager = pmf.getPersistenceManager();
        Transaction tx = deleteManager.currentTransaction();
        logger.trace("remove all instances of {}", type);
        long removed;
        try {
            tx.begin();
            Query query = deleteManager.newQuery("DELETE FROM "
                            + type.getName());
            removed = (Long) query.execute();
            tx.commit();
            logger.trace("successfully removed " + removed
                            + " entries of type " + type);
            return removed;
        } catch (JDOUserException t) {
            printSQLException("remove all instances of " + type, t);
            throw new BillingQueryException(t);
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

    /**
     * NB: This form of delete will only work if identity-type is NOT
     * "nondurable". Currently unused. (non-Javadoc)
     */
    @Override
    public <T> long remove(Class<T> type, String filter, Object... values)
                    throws BillingQueryException {
        return remove(type, filter, null, values);
    }

    /**
     * NB: This form of delete will only work if identity-type is NOT
     * "nondurable". Currently unused. (non-Javadoc)
     */
    @Override
    public <T> long remove(Class<T> type, String filter, String parameters,
                    Object... values) throws BillingQueryException {
        PersistenceManager deleteManager = pmf.getPersistenceManager();
        Transaction tx = deleteManager.currentTransaction();
        long removed;
        try {
            tx.begin();
            Query query = createQuery(deleteManager, type, filter, parameters);
            if (values == null) {
                removed = query.deletePersistentAll();
            } else {
                removed = query.deletePersistentAll(values);
            }
            tx.commit();
            logger.trace("successfully removed {} entries of type {}", removed,
                            type);
            return removed;
        } catch (JDOUserException t) {
            String message = "remove: " + type + ", " + filter + ", "
                            + parameters + ", " + Arrays.asList(values);
            printSQLException(message, t);
            throw new BillingQueryException(t);
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

    @Override
    public void setPropertiesPath(String propetiesPath) {
        propertiesPath = propetiesPath;
    }

    /**
     * From injection, but will be overwritten by any properties set in the
     * .properties resource or file.
     */
    private void addJdbcDNProperties() {
        if (jdbcDriver != null && !"".equals(jdbcDriver)) {
            properties.setProperty("datanucleus.ConnectionDriverName",
                            jdbcDriver);
        }
        if (!Strings.isNullOrEmpty(jdbcUrl)) {
            properties.setProperty("datanucleus.ConnectionURL", jdbcUrl);
        }
        if (!Strings.isNullOrEmpty(jdbcUser)) {
            properties.setProperty("datanucleus.ConnectionUserName", jdbcUser);
        }
        if (!Strings.isNullOrEmpty(jdbcPassword)) {
            properties.setProperty("datanucleus.ConnectionPassword",
                            jdbcPassword);
        }

        // datanucleus currently doesn't support setting the partition count
        // so the default value is used. This is '1' (from the
        // /bonecp-default-config.xml file in bonecp-<version>.jar)
        setPropertyIfValueSet(properties,
                        "datanucleus.connectionPool.minPoolSize",
                        minConnectionsPerPartition);
        setPropertyIfValueSet(properties,
                        "datanucleus.connectionPool.maxPoolSize",
                        maxConnectionsPerPartition);
    }

    /**
     * Tries to extract embedded messages from SQL exceptions.
     */
    private void printSQLException(String message, Throwable t) {
        printSQLException(new StringBuilder(message), t);
    }

    private void printSQLException(StringBuilder message, Throwable t) {
        if (!logger.isTraceEnabled()) {
            return;
        }

        if (t == null) {
            logger.trace(message.toString());
        } else if (t instanceof SQLException) {
            SQLException e = (SQLException) t;
            message.append("(")
                   .append(e.getClass())
                   .append(": ")
                   .append(e.getMessage())
                   .append("; Error code ")
                   .append(e.getErrorCode())
                   .append("; SQL state ")
                   .append(e.getSQLState())
                   .append(")");
            printSQLException(message, t.getCause());
        }
    }

    private void setPropertyIfValueSet(Properties properties, String key,
                    int value) {
        if (value != DUMMY_VALUE) {
            properties.setProperty(key, String.valueOf(value));
        }
    }
}
