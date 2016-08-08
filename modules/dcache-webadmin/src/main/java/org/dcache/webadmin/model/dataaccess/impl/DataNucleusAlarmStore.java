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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.dcache.alarms.dao.AlarmJDOUtils;
import org.dcache.alarms.dao.AlarmJDOUtils.AlarmDAOFilter;
import org.dcache.alarms.LogEntry;
import org.dcache.webadmin.model.dataaccess.LogEntryDAO;

/**
 * DataNucleus wrapper to underlying alarm store.<br>
 * <br>
 * Supports webadmin queries and updates.<br>
 * <br>
 *
 * @author arossi
 */
public class DataNucleusAlarmStore implements LogEntryDAO {
    private static final Logger logger
        = LoggerFactory.getLogger(DataNucleusAlarmStore.class);

    private PersistenceManagerFactory pmf;

    public void setPersistenceManagerFactory(PersistenceManagerFactory pmf) {
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

    public boolean isConnected() {
        return pmf != null && pmf.getPersistenceManager() != null;
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

        Transaction tx = deleteManager.currentTransaction();
        AlarmDAOFilter filter = AlarmJDOUtils.getIdFilter(selected);

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
            try {
                AlarmJDOUtils.rollbackIfActive(tx);
            } finally {
                deleteManager.close();
            }
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
            try {
                AlarmJDOUtils.rollbackIfActive(tx);
            } finally {
                updateManager.close();
            }
        }
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

        logger.debug("{}", action, e);
    }
}
