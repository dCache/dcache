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
package org.dcache.alarms.dao.impl;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.dcache.alarms.LogEntry;
import org.dcache.alarms.dao.AlarmJDOUtils;
import org.dcache.alarms.dao.AlarmJDOUtils.AlarmDAOFilter;
import org.dcache.alarms.dao.LogEntryDAO;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * DataNucleus wrapper to underlying alarm store.<br>
 * <br>
 */
public final class DataNucleusLogEntryStore implements LogEntryDAO, Runnable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final PersistenceManagerFactory pmf;

    /**
     * Optional cleaner daemon.
     */
    private boolean  cleanerEnabled;
    private int      cleanerSleepInterval;
    private TimeUnit cleanerSleepIntervalUnit;
    private int      cleanerDeleteThreshold;
    private TimeUnit cleanerDeleteThresholdUnit;
    private Thread   cleanerThread;

    public DataNucleusLogEntryStore(PersistenceManagerFactory pmf) {
        this.pmf = pmf;
    }

    @Override
    public Collection<LogEntry> get(AlarmDAOFilter filter) {
        PersistenceManager readManager = pmf.getPersistenceManager();
        if (readManager == null) {
            return Collections.emptyList();
        }

        Transaction tx = readManager.currentTransaction();

        try {
            tx.begin();
            Collection<LogEntry> result = AlarmJDOUtils.execute(readManager,
                                                                filter);
            logger.debug("got collection {}", result);
            Collection<LogEntry> detached = readManager.detachCopyAll(result);
            logger.debug("got detatched collection {}", detached);
            tx.commit();
            logger.debug("successfully executed get for filter {}", filter);
            return detached;
        } catch (JDOException t) {
            logJDOException("get", filter, t);
            return Collections.emptyList();
        } finally {
            try {
                AlarmJDOUtils.rollbackIfActive(tx);
            } finally {
                readManager.close();
            }
        }
    }

    public void initialize() {
        if (cleanerEnabled) {
            if (cleanerThread != null && cleanerThread.isAlive()) {
                return;
            }
            checkArgument(cleanerSleepInterval > 0);
            checkArgument(cleanerDeleteThreshold > 0);
            cleanerThread = new Thread(this, "alarm-cleanup-daemon");
            cleanerThread.start();
        }
    }

    public boolean isConnected() {
        return pmf != null && pmf.getPersistenceManager() != null;
    }

    @Override
    public void put(LogEntry entry) {
        PersistenceManager insertManager = pmf.getPersistenceManager();
        try {
            Transaction tx = insertManager.currentTransaction();
            tx.begin();
            try {
                Query query = insertManager.newQuery(LogEntry.class);
                query.setFilter("key==k");
                query.declareParameters("java.lang.String k");
                query.addExtension("datanucleus.query.resultCacheType", "none");
                query.getFetchPlan().setFetchSize(FetchPlan.FETCH_SIZE_OPTIMAL);

                Collection<LogEntry> dup
                                = (Collection<LogEntry>) query.executeWithArray(
                                entry.getKey());
                logger.trace("duplicate? {}", dup);

                if (dup != null && !dup.isEmpty()) {
                    if (dup.size() > 1) {
                        throw new RuntimeException
                                        ("data store inconsistency!"
                                                         + " more than one alarm with the same id: "
                                                         + entry.getKey());
                    }

                    LogEntry original = dup.iterator().next();
                    entry.setLastUpdate(original.getLastUpdate());

                    int received = original.getReceived();

                    if (original.isClosed()) {
                        /*
                         * this needs to be done or else newly arriving instances will
                         * not be tracked if this type has been closed previously
                         */
                        original.setClosed(false);

                        /*
                         * Treat the alarm as a new instance by restarting
                         * its history.  This guarantees a new alert will
                         * be sent and plugins called as if it were a
                         * first occurrence.
                         */
                        received = 1;
                    } else {
                        ++received;
                    }

                    original.setReceived(received);
                    entry.setReceived(received);

                    /*
                     * original is not detached so it will be updated on commit
                     */
                } else {
                    /*
                     * first instance of this alarm
                     */
                    logger.trace("makePersistent alarm, key={}",
                                 entry.getKey());
                    entry.setReceived(1);
                    insertManager.makePersistent(entry);
                    logger.trace("committing");
                }
                tx.commit();
                logger.debug("finished putting alarm, key={}", entry.getKey());
            } finally {
                AlarmJDOUtils.rollbackIfActive(tx);
            }
        } finally {
            /*
             * closing is necessary in order to avoid memory leaks
             */
            insertManager.close();
        }
    }

    @Override
    public long remove(Collection<LogEntry> selected) {
        if (selected.isEmpty()) {
            return 0;
        }

        PersistenceManager deleteManager = pmf.getPersistenceManager();
        if (deleteManager == null) {
            return 0;
        }

        /**
         * Too many deletes in a single transaction will
         * cause errors, even StackOverflow exceptions.
         * Break up into smaller batches.
         */
        long[] total = new long[]{0L};

        try {
            Iterables.partition(selected, 100).forEach((partition) -> {
                AlarmDAOFilter filter = AlarmJDOUtils.getIdFilter(partition);
                total[0] += doRemove(filter, deleteManager);
            });
        } finally {
            deleteManager.close();
        }

        return total[0];
    }

    @Override
    public void run() {
        try {
            while (isRunning()) {
                Long currentThreshold
                                = System.currentTimeMillis()
                                - cleanerDeleteThresholdUnit
                                .toMillis(cleanerDeleteThreshold);

                try {
                    long count = remove(currentThreshold);
                    logger.debug("removed {} closed alarms "
                                                 + "with timestamp prior to {}",
                                 count, new Date(currentThreshold));
                } catch (Exception e) {
                    logger.error("error in alarm cleanup: {}", e.getMessage());
                }

                cleanerSleepIntervalUnit.sleep(cleanerSleepInterval);
            }
        } catch (InterruptedException ignored) {
            logger.trace("cleaner thread interrupted ... exiting");
        }
    }

    public void setCleanerDeleteThreshold(int cleanerDeleteThreshold) {
        this.cleanerDeleteThreshold = cleanerDeleteThreshold;
    }

    public void setCleanerDeleteThresholdUnit(TimeUnit timeUnit) {
        cleanerDeleteThresholdUnit = checkNotNull(timeUnit);
    }

    public void setCleanerEnabled(boolean cleanerEnabled) {
        this.cleanerEnabled = cleanerEnabled;
    }

    public void setCleanerSleepInterval(int cleanerSleepInterval) {
        this.cleanerSleepInterval = cleanerSleepInterval;
    }

    public void setCleanerSleepIntervalUnit(TimeUnit timeUnit) {
        cleanerSleepIntervalUnit = checkNotNull(timeUnit);
    }

    public void shutdown() {
        if (cleanerThread != null) {
            cleanerThread.interrupt();
        }
    }

    @Override
    public long update(Collection<LogEntry> selected) {
        if (selected.isEmpty()) {
            return 0;
        }

        PersistenceManager updateManager = pmf.getPersistenceManager();
        if (updateManager == null) {
            return 0;
        }

        /**
         * Too many object updates in a single transaction will
         * cause errors, even StackOverflow exceptions.
         * Break up into smaller batches.
         */
        long[] total = new long[]{0L};

        try {
            Iterables.partition(selected, 100).forEach((partition) -> {
                total[0] += update(partition, updateManager);
            });
        } finally {
            updateManager.close();
        }

        return total[0];
    }

    private long update(Collection<LogEntry> selected, PersistenceManager updateManager) {
        Transaction tx = updateManager.currentTransaction();
        AlarmDAOFilter filter = AlarmJDOUtils.getIdFilter(selected);

        try {
            tx.begin();
            Collection<LogEntry> result = AlarmJDOUtils.execute(updateManager,
                                                                filter);
            logger.debug("got matching entries {}", result);
            long updated = result.size();

            Map<String, LogEntry> map = new HashMap<>();
            for (LogEntry e : selected) {
                map.put(e.getKey(), e);
            }

            for (LogEntry e : result) {
                e.update(map.get(e.getKey()));
            }

            /*
             * result is not detached so it will be updated on commit
             */
            tx.commit();
            logger.debug("successfully updated {} entries", updated);

            return updated;
        } catch (JDOException t) {
            logJDOException("update", filter, t);
            return 0;
        } finally {
            AlarmJDOUtils.rollbackIfActive(tx);
        }
    }

    private long doRemove(AlarmDAOFilter filter,
                          PersistenceManager deleteManager) {
        Transaction tx = deleteManager.currentTransaction();
        try {
            tx.begin();
            long removed = AlarmJDOUtils.delete(deleteManager, filter);
            tx.commit();
            logger.debug("successfully removed {} entries", removed);
            return removed;
        } catch (JDOException t) {
            logJDOException("remove", filter, t);
            return 0;
        } finally {
            AlarmJDOUtils.rollbackIfActive(tx);
        }
    }

    private boolean isRunning() {
        return cleanerThread != null && !cleanerThread.isInterrupted();
    }

    private void logJDOException(String action, AlarmDAOFilter filter,
                                 JDOException e) {
        /*
         * JDOException extends RuntimeException, but we treat it as
         * a checked exception here; the SQL error should neither
         * be dealt with by the client nor be propagated up in this case
         */
        if (filter == null) {
            logger.error("alarm data, failed to {}: {}",
                         action, e.getMessage());
        } else {
            logger.error("alarm data, failed to {}, {}: {}",
                         action, filter, e.getMessage());
        }
    }

    /**
     * Used only internally by the cleaner daemon (run()).
     */
    private long remove(Long threshold) throws Exception {
        PersistenceManager deleteManager = pmf.getPersistenceManager();
        try {
            AlarmDAOFilter filter
                            = AlarmJDOUtils.getDeleteBeforeFilter(threshold);
            return doRemove(filter, deleteManager);
        } finally {
            deleteManager.close();
        }
    }
}
