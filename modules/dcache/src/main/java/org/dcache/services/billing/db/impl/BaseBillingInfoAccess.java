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
package org.dcache.services.billing.db.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.exceptions.BillingInitializationException;
import org.dcache.services.billing.db.exceptions.BillingQueryException;

/**
 * Declares abstract methods for configuration, initialization and closing;
 * provides functionality for delayed commits, plus setters for JDBC arguments.
 *
 * @author arossi
 */
public abstract class BaseBillingInfoAccess implements IBillingInfoAccess {

    protected static final int DUMMY_VALUE = -1;

    protected static final String MAX_INSERTS_PROPERTY
        = "dbAccessMaxInsertsBeforeCommit";
    protected static final String MAX_TIMEOUT_PROPERTY
        = "dbAccessMaxTimeBeforeCommit";

    /**
     * Daemon which periodically flushes to the persistent store.
     */
    protected class TimedCommitter extends Thread {
        @Override
        public void run() {
            while (isRunning()) {
                try {
                    logger.trace("TimedCommitter thread sleeping");
                    Thread.sleep(maxTimeBeforeCommit * 1000L);
                    logger.trace("TimedCommitter thread calling doCommitIfNeeded");
                } catch (InterruptedException ignored) {
                    logger.trace("TimedCommitter thread sleep interrupted");
                }

                try {
                    doCommitIfNeeded(true);
                } catch (BillingQueryException t) {
                    synchronized (this) {
                        logger.error("failed periodic commit! " +
                                     "{} billing records may have been lost",
                                     insertCount);
                        logger.trace("timed commit failure", t);
                    }
                }
            }
            logger.trace("TimedCommitter thread doCommitIfNeeded");
            try {
                doCommitIfNeeded(true);
            } catch (BillingQueryException t) {
                synchronized (this) {
                    logger.error("failed final commit! " +
                                 "{} billing records may have been lost",
                                 insertCount);
                    logger.trace("timed commit failure", t);
                }
            }
            logger.trace("TimedCommitter thread exiting");
        }
    }

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected Properties properties;

    /**
     * Injected properties
     */
    protected String propertiesPath;
    protected String jdbcDriver;
    protected String jdbcUrl;
    protected String jdbcUser;
    protected String jdbcPassword;
    protected int partitionCount = DUMMY_VALUE;
    protected int maxConnectionsPerPartition = DUMMY_VALUE;
    protected int minConnectionsPerPartition = DUMMY_VALUE;

    /**
     * for delayed/batched commits on put (performance optimization)
     */
    protected int insertCount;
    protected int maxInsertsBeforeCommit = 1;
    protected int maxTimeBeforeCommit;

    private Thread flushD;
    private boolean running;

    /**
     * Main initialization method. Calls the internal configure method and
     * possibly starts the flush daemon.
     */
    @Override
    public final void initialize() throws BillingInitializationException {
        properties = new Properties();
        initializeInternal();

        logger.trace("maxInsertsBeforeCommit {}", maxInsertsBeforeCommit);
        logger.trace("maxTimeBeforeCommit {}", maxTimeBeforeCommit);

        /*
         * if using delayed commits, run a flush thread
         */
        if (maxTimeBeforeCommit > 0) {
            flushD = new TimedCommitter();
            setRunning(true);
            flushD.start();
        }
    }

    /**
     * Shuts down flush daemon.
     */
    @Override
    public void close() {
        setRunning(false);
        if (flushD != null) {
            try {
                logger.trace("interrupting flush daemon");
                flushD.interrupt();
                logger.trace("waiting for flush daemon to exit");
                flushD.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.trace("close interrupted");
            }
        }
        logger.trace("close exiting");
    }

    /**
     * Does any necessary internal initialization
     */
    protected abstract void initializeInternal()
                    throws BillingInitializationException;

    protected synchronized boolean isRunning() {
        return running;
    }

    protected synchronized void setRunning(boolean running) {
        this.running = running;
    }

    protected abstract void doCommitIfNeeded(boolean force)
                    throws BillingQueryException;

    public Properties getProperties() {
        return properties;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public void setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
    }

    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    public void setMaxInsertsBeforeCommit(int maxInsertsBeforeCommit) {
        this.maxInsertsBeforeCommit = maxInsertsBeforeCommit;
    }

    public void setMaxTimeBeforeCommit(int maxTimeBeforeCommit) {
        this.maxTimeBeforeCommit = maxTimeBeforeCommit;
    }

    public void setPropertiesPath(String propetiesPath) {
        this.propertiesPath = propetiesPath;
    }

    public void setPartitionCount(int count) {
        partitionCount = count;
    }

    public void setMaxConnectionsPerPartition(int count) {
        maxConnectionsPerPartition = count;
    }

    public void setMinConnectionsPerPartition(int count) {
        minConnectionsPerPartition = count;
    }
}
