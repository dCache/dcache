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
package org.dcache.webadmin.model.dataaccess.impl;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.dcache.alarms.dao.LogEntry;
import org.dcache.webadmin.model.dataaccess.ILogEntryDAO;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.model.util.AlarmJDOUtils;
import org.dcache.webadmin.model.util.AlarmJDOUtils.AlarmDAOFilter;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * DataNucleus wrapper to underlying alarm store.<br>
 * <br>
 * Supports webadmin queries and updates.<br>
 * <br>
 * Note that this implementation is agnostic as to actual storage type;
 * non-RDBMs plugins may exhibit performance slowdowns as the store fills up, so
 * the administrator may need to do periodic deletes manually or through the
 * adminstrative interface.
 *
 * @author arossi
 */
public class DataNucleusAlarmStore implements ILogEntryDAO, Runnable {
    private static final Logger logger
        = LoggerFactory.getLogger(DataNucleusAlarmStore.class);

    private final Properties properties;
    private final File xml;
    private final boolean active;
    private final boolean usesXml;

    /**
     * how long (in milliseconds) the alarm cleaner should sleep before running.
     */
    private final long cleanerSleepInterval;

    /**
     * timestamp to use in query for deletion of closed alarms. represents time
     * (in milliseconds) before time at which alarm thread is awakened.
     */
    private final long cleanerDeleteThreshold;

    private PersistenceManagerFactory pmf;
    private Thread cleanerThread;

    private static void rollbackIfActive(Transaction tx) {
        if (tx.isActive()) {
            tx.rollback();
        }
    }

    public DataNucleusAlarmStore(String xmlPath, Properties properties,
                    boolean enableCleaner, int cleanerSleepInterval,
                    int cleanerDeleteThreshold) {
        this.cleanerSleepInterval = TimeUnit.HOURS.toMillis(cleanerSleepInterval);
        this.cleanerDeleteThreshold = TimeUnit.HOURS.toMillis(cleanerDeleteThreshold);

        this.properties = properties;
        properties.put("javax.jdo.PersistenceManagerFactoryClass",
                        "org.datanucleus.api.jdo.JDOPersistenceManagerFactory");

        String url = properties.getProperty("datanucleus.ConnectionURL");

        if (Strings.isNullOrEmpty(url)) {
           active = false;
           xml = null;
           usesXml = false;
        } else {
           active = true;

           usesXml = url.startsWith("xml:");
           xml = Strings.isNullOrEmpty(xmlPath) ? null : new File(xmlPath);
           checkArgument(!(usesXml && xml == null));

           if (enableCleaner) {
               checkArgument(cleanerSleepInterval > 0);
               checkArgument(cleanerDeleteThreshold > 0);
               cleanerThread = new Thread(this, "alarm-cleanup-daemon");
           }
        }
    }

    @Override
    public Collection<LogEntry> get(AlarmDAOFilter filter) throws DAOException {
        PersistenceManager readManager = getManager();
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
        } catch (Exception t) {
            String message = "get, filter = " + filter + ": "
                            + t.getLocalizedMessage();
            throw new DAOException(message, t);
        } finally {
            rollbackIfActive(tx);
            /*
             * closing is necessary in order to avoid memory leaks
             */
            readManager.close();
        }
    }

    public void initialize() {
        if (!active) {
            return;
        }

        if (cleanerThread != null && !cleanerThread.isAlive()) {
            cleanerThread.start();
        }
    }

    public boolean isConnected() {
        try {
            return active && (getManager() != null);
        } catch (DAOException t) {
            return false;
        }
    }

    @Override
    public long remove(Collection<LogEntry> selected) throws DAOException {
        if (selected.isEmpty()) {
            return 0;
        }

        PersistenceManager deleteManager = getManager();
        if (deleteManager == null) {
            return 0;
        }

        Transaction tx = deleteManager.currentTransaction();
        AlarmDAOFilter filter = AlarmJDOUtils.getIdFilter(selected);

        try {
            tx.begin();
            long removed = AlarmJDOUtils.delete(deleteManager, filter);
            tx.commit();
            logger.debug("successfully removed {} entries", removed);
            return removed;
        } catch (Exception t) {
            String message = "remove: " + filter + ": "
                            + t.getLocalizedMessage();
            throw new DAOException(message, t);
        } finally {
            rollbackIfActive(tx);
            /*
             * closing is necessary in order to avoid memory leak in the
             * persistence manager factory
             */
            deleteManager.close();
        }
    }

    @Override
    public void run() {
        while (isRunning()) {
            Long currentThreshold
                = System.currentTimeMillis() - cleanerDeleteThreshold;

            try {
                long count = remove(currentThreshold);
                logger.debug("removed {} closed alarms with timestamp prior to {}",
                                count, new Date(currentThreshold));
            } catch (DAOException e) {
                logger.error("error in alarm cleanup: {}", e.getLocalizedMessage());
            }

            try {
                Thread.sleep(cleanerSleepInterval);
            } catch (InterruptedException ignored) {
                logger.trace("cleaner thread interrupted ... exiting");
                break;
            }
        }
    }

    public synchronized void shutDown() {
        if (cleanerThread != null ) {
            cleanerThread.interrupt();
        }
    }

    @Override
    public long update(Collection<LogEntry> selected) throws DAOException {
        if (selected.isEmpty()) {
            return 0;
        }

        PersistenceManager updateManager = getManager();
        if (updateManager == null) {
            return 0;
        }

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
        } catch (Exception t) {
            String message = "update: " + filter + ": "
                            + t.getLocalizedMessage();
            throw new DAOException(message, t);
        } finally {
            rollbackIfActive(tx);
            /*
             * closing is necessary in order to avoid memory leak in the
             * persistence manager factory
             */
            updateManager.close();
        }
    }

    private PersistenceManager getManager() throws DAOException {
        if (usesXml) {
            if (!xml.exists() || !xml.isFile()) {
                if (pmf != null) {
                    pmf.close();
                    pmf = null;
                }
                return null;
            }
        }

        if (pmf == null) {
            pmf = JDOHelper.getPersistenceManagerFactory(properties);
        }

        return pmf.getPersistenceManager();
    }

    private synchronized boolean isRunning() {
        return active && cleanerThread != null && !cleanerThread.isInterrupted();
    }

    /**
     * Used only internally by the cleaner daemon (run()).
     */
    private long remove(Long threshold) throws DAOException {
        PersistenceManager deleteManager = getManager();
        if (deleteManager == null) {
            return 0;
        }

        Transaction tx = deleteManager.currentTransaction();
        AlarmDAOFilter filter = AlarmJDOUtils.getDeleteBeforeFilter(threshold);
        try {
            tx.begin();
            long removed = AlarmJDOUtils.delete(deleteManager, filter);
            tx.commit();
            logger.debug("successfully removed {} entries", removed);
            return removed;
        } catch (Exception t) {
            String message = "remove: " + filter + ": "
                            + t.getLocalizedMessage();
            throw new DAOException(message, t);
        } finally {
            rollbackIfActive(tx);
            /*
             * closing is necessary in order to avoid memory leak in the
             * persistence manager factory
             */
            deleteManager.close();
        }
    }
}
