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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.FetchPlan;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Properties;

import org.dcache.alarms.dao.ILogEntryDAO;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.alarms.dao.LogEntryStorageException;

/**
 * DataNucleus wrapper to underlying alarm store.<br>
 * <br>
 * Supports the logging appender.
 *
 * @author arossi
 */
public class DataNucleusLogEntryStore implements ILogEntryDAO {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final String xmlPath;
    private final Properties properties = new Properties();
    private PersistenceManagerFactory pmf;

    public DataNucleusLogEntryStore(String xmlPath, Properties properties)
                    throws LogEntryStorageException {
        this.xmlPath = xmlPath;
        this.properties.putAll(properties);
        initialize();
    }

    @Override
    public void put(LogEntry entry) throws LogEntryStorageException {
        PersistenceManager insertManager = pmf.getPersistenceManager();
        Transaction tx = insertManager.currentTransaction();
        Query query = insertManager.newQuery(LogEntry.class);
        query.setFilter("key==k");
        query.declareParameters("java.lang.String k");
        query.addExtension("datanucleus.query.resultCacheType", "none");
        /*
         * looks like DataNucleus 3.1.3+ needs this to get the most recent
         * updates made from another JVM
         */
        query.setIgnoreCache(true);
        query.getFetchPlan().setFetchSize(FetchPlan.FETCH_SIZE_OPTIMAL);

        try {
            tx.begin();
            Collection<LogEntry> dup
                = (Collection<LogEntry>) query.executeWithArray(entry.getKey());
            logger.trace("duplicate? {}", dup);
            if (dup != null && !dup.isEmpty()) {
                if (dup.size() > 1) {
                    throw new LogEntryStorageException
                        ("data store inconsistency!"
                          + " more than one alarm with the same id: "
                                                    + entry.getKey());
                }
                LogEntry original = dup.iterator().next();
                original.setLastUpdate(entry.getLastUpdate());
                original.setReceived(original.getReceived() + 1);
		/*
                 * this needs to be done or else newly arriving instances
                 * will not be tracked if this type has been closed
                 * previously
                 */
                original.setClosed(false);
                /*
                 * original is not detached so it will be updated on commit
                 */
            } else {
                /*
                 * first instance of this alarm
                 */
                logger.trace("makePersistent alarm, key={}", entry.getKey());
                insertManager.makePersistent(entry);
                logger.trace("committing");
            }
            tx.commit();
            logger.debug("finished putting alarm, key={}", entry.getKey());
        } catch (Throwable t) {
            if (tx.isActive()) {
                tx.rollback();
            }
            String message = "committing alarm, key=" + entry.getKey();
            throw new LogEntryStorageException(message, t);
        } finally {
            /*
             * closing is necessary in order to avoid memory leaks
             */
            insertManager.close();
        }
    }

    private void initialize() throws LogEntryStorageException {
        try {
            if (properties.getProperty("datanucleus.ConnectionURL")
                            .startsWith("xml:")) {
                initializeXmlFile();
            }
            properties.put("javax.jdo.PersistenceManagerFactoryClass",
                            "org.datanucleus.api.jdo.JDOPersistenceManagerFactory");
            pmf = JDOHelper.getPersistenceManagerFactory(properties);
        } catch (IOException t) {
            throw new LogEntryStorageException(t);
        }
    }

    /**
     * Checks for the existence of the file and creates it if not. Note that
     * existing files are not validated against any schema, explicit or
     * implicit. If the parent does not exist, an exception will be thrown.
     */
    private void initializeXmlFile() throws IOException {
        File file = new File(xmlPath);
        if (!file.exists()) {
            if (!file.getParentFile().isDirectory()) {
                String parent = file.getParentFile().getAbsolutePath();
                throw new FileNotFoundException(parent + " is not a directory");
            }
            try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
                pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                pw.println("<entries></entries>");
                pw.flush();
            }
        }
    }
}
