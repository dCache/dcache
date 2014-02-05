package org.dcache.services.billing.db;

import junit.framework.TestCase;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.dcache.services.billing.db.impl.BaseBillingInfoAccess;
import org.dcache.services.billing.db.impl.datanucleus.DataNucleusBillingInfo;

/**
 * @author arossi
 *
 */
public abstract class BaseBillingInfoAccessTest extends TestCase {

    private static final String URL = "jdbc:hsqldb:mem:billing_test";
    private static final String USER = "sa";
    private static final String PASS = "";

    protected InfoMessageGenerator messageGenerator;
    protected Random r = new Random(System.currentTimeMillis());

    private DataNucleusBillingInfo access;

    @Override
    protected void setUp() throws Exception {
        messageGenerator = new InfoMessageGenerator();
        createAccess();
    }

    @Override
    protected void tearDown() throws Exception {
        close();
    }

    protected synchronized IBillingInfoAccess getAccess() {
        return access;
    }

    private Properties properties() throws IOException {
        Properties properties = new Properties();
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
        return properties;
    }

    private synchronized void createAccess() throws Exception {
        try {
            JDOPersistenceManagerFactory pmf = new JDOPersistenceManagerFactory(properties());
            pmf.setConnectionFactory(new DriverManagerDataSource(URL, USER, PASS));

            access = new DataNucleusBillingInfo();
            access.setDelegateType("org.dcache.services.billing.db.impl.DirectQueueDelegate");
            access.setMaxBatchSize(1000);
            access.setMaxQueueSize(1000);
            access.setPersistenceManagerFactory(pmf);
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
        access.remove(clzz);
    }
}
