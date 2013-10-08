package org.dcache.services.billing.db.impl;

import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.IPlotData;
import org.dcache.services.billing.db.exceptions.BillingInitializationException;
import org.dcache.services.billing.db.exceptions.BillingStorageException;

/**
 * Framework for database access; composes delegate for handling inserts.
 *
 * @author arossi
 */
public abstract class BaseBillingInfoAccess implements IBillingInfoAccess {
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

    private String delegateType;
    private QueueDelegate delegate;
    private int maxQueueSize;
    private int maxBatchSize;
    private boolean dropMessagesAtLimit;

    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }

    public abstract void commit(Collection<IPlotData> data)
                    throws BillingStorageException;

    public Properties getProperties() {
        return properties;
    }

    public void initialize() throws BillingInitializationException {
        logger.debug("access type: {}", this.getClass().getName());

        properties = new Properties();
        initializeInternal();

        /*
         * it is possible to configure the DAO layer for read-only,
         * meaning there is no insert delegate; see #put() below
         */
        if (delegateType != null) {
            try {
                Class clzz = Class.forName(delegateType);
                delegate = (QueueDelegate) clzz.newInstance();
                delegate.setDropMessagesAtLimit(dropMessagesAtLimit);
                delegate.setMaxQueueSize(maxQueueSize);
                delegate.setMaxBatchSize(maxBatchSize);
                delegate.setCallback(this);
                delegate.initialize();
                logger.debug("delegate type: {}", clzz);
            } catch (Exception e) {
                throw new BillingInitializationException(e.getMessage(),
                                e.getCause());
            }
        }
    }

    public void put(IPlotData data) throws BillingStorageException {
        if (delegate == null) {
            logger.warn("attempting to insert data but database access has not"
                            + " been initialized to handle inserts; please set "
                            + "billing.db.inserts.queue-delegate.type property");
            return;
        }
        delegate.handlePut(data);
    }

    public void setDelegateType(String delegateType) {
        this.delegateType = Strings.emptyToNull(delegateType);
    }

    public void setDropMessagesAtLimit(boolean dropMessagesAtLimit) {
        this.dropMessagesAtLimit = dropMessagesAtLimit;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public void setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
    }

    public void setPropertiesPath(String propetiesPath) {
        this.propertiesPath = propetiesPath;
    }

    protected abstract void initializeInternal()
                    throws BillingInitializationException;

}
