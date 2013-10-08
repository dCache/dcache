package org.dcache.services.billing.db;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.dcache.services.billing.db.exceptions.BillingQueryException;
import org.dcache.services.billing.db.impl.BaseBillingInfoAccess;
import org.dcache.services.billing.db.impl.datanucleus.DataNucleusBillingInfo;

/**
 * @author arossi
 *
 */
public abstract class BaseBillingInfoAccessTest extends TestCase {

    private static final String URL = "jdbc:hsqldb:mem:billing_test";
    private static final String DRIVER = "org.hsqldb.jdbcDriver";
    private static final String USER = "sa";
    private static final String PASS = "";

    protected InfoMessageGenerator messageGenerator;
    protected Random r = new Random(System.currentTimeMillis());

    private File testProperties;
    private BaseBillingInfoAccess access;

    @Override
    protected void setUp() throws Exception {
        Class.forName(DRIVER);
        messageGenerator = new InfoMessageGenerator();
        setProperties();
        createAccess();
    }

    @Override
    protected void tearDown() throws Exception {
        if (testProperties != null) {
            testProperties.delete();
        }
        close();
    }

    protected synchronized IBillingInfoAccess getAccess() {
        return access;
    }

    private void setProperties() throws IOException {
        Properties properties = new Properties();
        properties.setProperty("javax.jdo.PersistenceManagerFactoryClass",
                        "org.datanucleus.api.jdo.JDOPersistenceManagerFactory");
        properties.setProperty("datanucleus.connectionPoolingType", "BoneCP");
        properties.setProperty("datanucleus.connectionPool.maxIdle", "1");
        properties.setProperty("datanucleus.connectionPool.minIdle", "1");
        properties.setProperty("datanucleus.connectionPool.maxActive", "1");
        properties.setProperty("datanucleus.connectionPool.maxWait", "60");
        properties.setProperty("datanucleus.connectionPool.minPoolSize", "1");
        properties.setProperty("datanucleus.connectionPool.maxPoolSize", "5");
        properties.setProperty("datanucleus.autoCreateSchema", "true");
        properties.setProperty("datanucleus.autoCreateTables", "true");
        properties.setProperty("datanucleus.autoCreateColumns", "true");
        properties.setProperty("datanucleus.autoCreateConstraints", "true");
        properties.setProperty("datanucleus.validateTables", "true");
        properties.setProperty("datanucleus.validateConstraints", "true");
        properties.setProperty("datanucleus.validateColumns", "true");
        properties.setProperty("datanucleus.rdbms.CheckExistTablesOrViews",
                        "true");
        properties.setProperty("datanucleus.rdbms.initializeColumnInfo", "None");
        properties.setProperty("datanucleus.identifier.case", "LowerCase");
        properties.setProperty("javax.jdo.option.Optimistic", "true");
        properties.setProperty("javax.jdo.option.NontransactionalRead", "true");
        properties.setProperty("javax.jdo.option.RetainValues", "true");
        properties.setProperty("javax.jdo.option.Multithreaded", "true");
        properties.setProperty("datanucleus.autoStartMechanism", "false");
        properties.setProperty("datanucleus.manageRelationships", "false");
        properties.setProperty("datanucleus.rdbms.statementBatchLimit", "-1");
        properties.setProperty("datanucleus.detachAllOnCommit", "false");
        properties.setProperty("datanucleus.persistenceByReachabilityAtCommit",
                        "false");
        properties.setProperty("datanucleus.query.jdoql.allowAll", "true");

        testProperties = File.createTempFile("test", ".properties");
        properties.store(new FileOutputStream(testProperties), "testProperties");
    }

    private synchronized void createAccess() throws Exception {
        try {
            access = new DataNucleusBillingInfo();
            access.setPropertiesPath(testProperties.getAbsolutePath());
            access.setJdbcDriver(DRIVER);
            access.setJdbcUrl(URL);
            access.setJdbcUser(USER);
            access.setJdbcPassword(PASS);
            access.setDelegateType("org.dcache.services.billing.db.impl.DirectQueueDelegate");
            access.setMaxBatchSize(1000);
            access.setMaxQueueSize(1000);
            access.initialize();
        } catch (Throwable t) {
            throw new Exception(t.getMessage(), t.getCause());
        }
    }

    private synchronized void close() {
        if (access != null && access instanceof BaseBillingInfoAccess) {
            access.close();
        }
        access = null;
    }

    protected void cleanup(Class<?> clzz) {
        try {
            access.remove(clzz);
        } catch (BillingQueryException t) {
            t.printStackTrace();
        }
    }
}
