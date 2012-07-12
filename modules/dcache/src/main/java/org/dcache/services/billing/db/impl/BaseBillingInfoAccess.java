package org.dcache.services.billing.db.impl;

import java.util.Properties;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.exceptions.BillingInitializationException;
import org.dcache.services.billing.db.exceptions.BillingStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Declares abstract methods for configuration, initialization and closing;
 * provides functionality for delayed commits, plus setters for JDBC arguments.
 *
 * @see IBillingInfoAccess
 *
 * @author arossi
 */
public abstract class BaseBillingInfoAccess implements IBillingInfoAccess {

    protected static final String MAX_INSERTS_PROPERTY = "dbAccessMaxInsertsBeforeCommit";
    protected static final String MAX_TIMEOUT_PROPERTY = "dbAccessMaxTimeBeforeCommit";

    /**
     * Daemon which periodically flushes to the persistent store.
     *
     * @author arossi
     */
    protected class TimedCommitter extends Thread {
        @Override
        public void run() {
            while (isRunning()) {
                try {
                    logger.debug("{} sleeping", this);
                    Thread.sleep(maxTimeBeforeCommit*1000L);
                    logger.debug("{} calling doCommitIfNeeded", this);
                    doCommitIfNeeded(true);
                } catch (InterruptedException ignored) {
                }
            }
            logger.debug("{} calling doCommitIfNeeded", this);
            doCommitIfNeeded(true);
            logger.debug("{} exiting", this);
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

    /*
     * for delayed/batched commits on put (performance optimization)
     */
    protected int insertCount = 0;
    protected int maxInsertsBeforeCommit = 1;
    protected int maxTimeBeforeCommit = 0;

    private Thread flushD;
    private boolean running;

    /**
     * Main initialization method. Calls the internal configure method and
     * possibly starts the flush daemon.
     *
     * @throws BillingInitializationException
     */
    @Override
    public final void initialize() throws BillingInitializationException {
        properties = new Properties();
        initializeInternal();

        logger.debug("maxInsertsBeforeCommit {}", maxInsertsBeforeCommit);
        logger.debug("maxTimeBeforeCommit {}", maxTimeBeforeCommit);

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
                logger.debug("interrupting flush daemon");
                flushD.interrupt();
                logger.debug("waiting for flush daemon to exit");
                flushD.join();
            } catch (InterruptedException ignored) {
            }
        }
        logger.debug("{} close exiting", this);
    }

    /**
     * Does any necessary internal initialization
     *
     * @throws BillingInitializationException
     */
    protected abstract void initializeInternal() throws BillingInitializationException;

    /**
     * @return if timer thread is running
     */
    protected synchronized boolean isRunning() {
        return running;
    }

    /**
     * @param running
     */
    protected synchronized void setRunning(boolean running) {
        this.running = running;
    }

    /**
     * Commits if threshold is reached.
     *
     * @param force
     *            force commit if true
     */
    protected abstract void doCommitIfNeeded(boolean force);

    /**
     * @return the properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * @param jdbcDriver
     *            the jdbcDriver to set
     */
    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    /**
     * @param jdbcUrl
     *            the jdbcUrl to set
     */
    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * @param jdbcUser
     *            the jdbcUser to set
     */
    public void setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
    }

    /**
     * @param jdbcPassword
     *            the jdbcPassword to set
     */
    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    /**
     * @param maxInsertsBeforeCommit
     *            the maxInsertsBeforeCommit to set
     */
    public void setMaxInsertsBeforeCommit(int maxInsertsBeforeCommit) {
        this.maxInsertsBeforeCommit = maxInsertsBeforeCommit;
    }

    /**
     * @param maxTimeBeforeCommit
     *            the maxTimeBeforeCommit to set
     */
    public void setMaxTimeBeforeCommit(int maxTimeBeforeCommit) {
        this.maxTimeBeforeCommit = maxTimeBeforeCommit;
    }

    /**
     * @param propetiesPath
     *            the propetiesPath to set
     */
    public void setPropertiesPath(String propetiesPath) {
        this.propertiesPath = propetiesPath;
    }
}
