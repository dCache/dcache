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

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import javax.jdo.JDOCanRetryException;
import javax.jdo.JDODataStoreException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import org.datanucleus.FetchPlan;
import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.IHistogramData;
import org.dcache.services.billing.db.exceptions.RetryException;
import org.dcache.services.billing.db.impl.AbstractBillingInfoAccess;
import org.springframework.beans.factory.annotation.Required;

/**
 * Implements {@link IBillingInfoAccess} using <href a="http://www.datanucleus.org">DataNucleus</a>.
 *
 * @see AbstractBillingInfoAccess
 */
public class DataNucleusBillingInfo extends AbstractBillingInfoAccess {

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

    private PersistenceManagerFactory pmf;
    private long truncationCutoff;
    private TimeUnit truncationCutoffUnit;

    @Override
    public void commit(Collection<IHistogramData> data)
          throws RetryException {
        PersistenceManager insertManager = pmf.getPersistenceManager();
        Transaction tx = insertManager.currentTransaction();
        try {
            tx.begin();
            insertManager.makePersistentAll(data);
            tx.commit();
        } catch (JDOCanRetryException t) {
            throw new RetryException(t);
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
    public <T> Collection<T> get(Class<T> type) {
        return get(type, null, null, (Object) null);
    }

    @Override
    public <T> Collection<T> get(Class<T> type, String filter, Object... values) {
        return get(type, filter, null, values);
    }

    @Override
    public <T> Collection<T> get(Class<T> type, String filter,
          String parameters, Object... values) {
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
                  values == null ? null : Arrays.asList(values));
            return detached;
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

    /**
     * Does bulk deletes. Properties file must set
     * <code>datanucleus.query.jdoql.allowAll=true</code> (non-Javadoc)
     */
    @Override
    public <T> long remove(Class<T> type) {
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
            logger.trace("successfully removed {} entries of type {}", removed, type);
            return removed;
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
     * NB: This form of delete will only work if identity-type is NOT "nondurable". Currently
     * unused. (non-Javadoc)
     */
    @Override
    public <T> long remove(Class<T> type, String filter, Object... values) {
        return remove(type, filter, null, values);
    }

    /**
     * NB: This form of delete will only work if identity-type is NOT "nondurable". Currently
     * unused. (non-Javadoc)
     */
    @Override
    public <T> long remove(Class<T> type, String filter, String parameters,
          Object... values) {
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

    @Required
    public void setPersistenceManagerFactory(PersistenceManagerFactory pmf) {
        this.pmf = pmf;
    }

    @Required
    public void setTruncationCutoff(long truncationCutoff) {
        this.truncationCutoff = truncationCutoff;
    }

    @Required
    public void setTruncationCutoffUnit(TimeUnit truncationCutoffUnit) {
        this.truncationCutoffUnit = truncationCutoffUnit;
    }

    @Override
    public void aggregateDaily() {
        logger.info("executing daily aggregation procedure.");
        executeStoredProcedure("f_billing_daily_summary()");
        logger.info("finished executing daily aggregation procedure.");
    }

    @Override
    public void truncateFineGrained() {
        long before = System.currentTimeMillis()
              - truncationCutoffUnit.toMillis(truncationCutoff);
        logger.info("executing fine grained table trunction of rows before {}.",
              new Date(before));
        Object result = executeStoredProcedure("f_truncate_fine_grained_info(" + before + ")");
        logger.info("finished executing fine grained table trunction of rows, "
              + "removed a total of {} rows.", result);
    }

    private Object executeStoredProcedure(String name) {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();
        Object result;

        try {
            tx.begin();
            /*
             * REVISIT
             *
             * The recommended recipe to execute a stored procedure using
             * DataNucleus JDO API is to call
             *
             *          Query query = pm.newQuery("STOREDPROC",name);
             *
             * http://www.datanucleus.org/products/accessplatform_4_1/jdo/stored_procedures.html
             *
             * Unfortunately, currently this call translates into a "CALL name"
             * query on a DB backend and therefore does not work for postgresql.
             *
             * http://www.datanucleus.org/servlet/forum/viewthread_thread,7968
             *
             * Until this is fixed, we must use an SQL pass-through hack.
             *
             * NOTE:  if the procedure is a function, then the name
             * must be expressed as:  'function_name(p1, p2 ...)'.
             *
             */
            Query query = pm.newQuery("javax.jdo.query.SQL",
                  "SELECT " + name);
            try {
                result = query.execute();
            } catch (JDODataStoreException ignore) {
                query = pm.newQuery("javax.jdo.query.SQL",
                      "CALL " + name);
                result = query.execute();
            }

            tx.commit();
        } finally {
            try {
                rollbackIfActive(tx);
            } finally {
                pm.close();
            }
        }

        return result;
    }
}
