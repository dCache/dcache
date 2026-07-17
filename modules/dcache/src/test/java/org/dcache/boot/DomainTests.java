package org.dcache.boot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;

import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.dcache.util.configuration.ConfigurationProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DomainTests {

    private static final String DOMAIN_NAME = "domainName";

    // Domain always has a "domain.name" property who's value is the domain's name.
    private static final String PROPERTY_DOMAIN_NAME_KEY = "dcache.domain.name";
    private static final String PROPERTY_DOMAIN_NAME_VALUE = DOMAIN_NAME;

    Domain _domain;
    private ConfigurationProperties _domainProperties;

    private TestingServer zkTestServer;
    private int tlsPort;
    private String caPath;
    private String curatorKeyPath;
    private String curatorCertPath;

    @Before
    public void setUp() throws Exception {

        caPath = Objects.requireNonNull(getClass().getResource("/zookeeper-tls/certificates/")).getFile();
        curatorKeyPath = Objects.requireNonNull(getClass().getResource("/zookeeper-tls/curator-key.pem")).getFile();
        curatorCertPath = Objects.requireNonNull(getClass().getResource("/zookeeper-tls/curator-cert.pem")).getFile();
        String caFile = Objects.requireNonNull(getClass().getResource("/zookeeper-tls/ca-cert.pem")).getFile();
        String zkCombined = Objects.requireNonNull(getClass().getResource("/zookeeper-tls/zookeeper-combined.pem")).getFile();

        tlsPort = InstanceSpec.getRandomPort();
        HashMap<String, Object> zkProperties = new HashMap<>();
        zkProperties.put("clientPort", "0");
        zkProperties.put("secureClientPort", String.valueOf(tlsPort));
        zkProperties.put("serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");
        zkProperties.put("ssl.keyStore.location", zkCombined);
        zkProperties.put("ssl.keyStore.type", "PEM");
        zkProperties.put("ssl.trustStore.location", caFile);
        zkProperties.put("ssl.trustStore.type", "PEM");
        zkProperties.put("ssl.clientAuth", "NEED");

        InstanceSpec spec = new InstanceSpec(null, -1, -1,
                -1, true, -1, -1,
                -1, zkProperties);
        zkTestServer = new TestingServer(spec, true);

        _domainProperties = new ConfigurationProperties(new Properties());
        _domainProperties.setProperty("dcache.zookeeper.max-retries", "3");
        _domainProperties.setProperty("dcache.zookeeper.initial-retry-delay", "1");
        _domainProperties.setProperty("dcache.zookeeper.initial-retry-delay.unit", "SECONDS");
        _domainProperties.setProperty("dcache.zookeeper.connection-timeout", "15");
        _domainProperties.setProperty("dcache.zookeeper.connection-timeout.unit", "SECONDS");
        _domainProperties.setProperty("dcache.zookeeper.session-timeout", "60");
        _domainProperties.setProperty("dcache.zookeeper.session-timeout.unit", "SECONDS");
        _domainProperties.setProperty("dcache.zookeeper.connection", "localhost:2181");
        _domainProperties.setProperty("dcache.zookeeper.tls.enabled", "true");
        _domainProperties.setProperty("dcache.authn.crl-mode", "IF_VALID");
        _domainProperties.setProperty("dcache.authn.ocsp-mode", "IF_AVAILABLE");

    }

    @Test
    public void testCreateWithNoGlobalProperties() {
        _domain = new Domain(DOMAIN_NAME, _domainProperties);
        assertEquals(DOMAIN_NAME, _domain.getName());
        assertDomainServicesCount(0);
        assertDomainPropertyValue(PROPERTY_DOMAIN_NAME_KEY, PROPERTY_DOMAIN_NAME_VALUE);
    }


    @Test
    public void testCreateWithAGlobalProperty() {
        String globalPropertyKey = "global.property";
        String globalPropertyValue = "global property value";
        ConfigurationProperties globalProperties = new ConfigurationProperties(new Properties());
        globalProperties.setProperty(globalPropertyKey, globalPropertyValue);

        _domain = new Domain(DOMAIN_NAME, globalProperties);

        assertEquals(DOMAIN_NAME, _domain.getName());
        assertDomainServicesCount(0);

        // NB global properties are imported into Properties as "default" values, so
        // are not included in the property count.
        assertDomainPropertyValue(PROPERTY_DOMAIN_NAME_KEY, PROPERTY_DOMAIN_NAME_VALUE);
        assertDomainPropertyValue(globalPropertyKey, globalPropertyValue);
    }

    @Test
    public void testCreateCuratorFrameworkEmptyTLSConfigThrows() {
        _domain = new Domain(DOMAIN_NAME, _domainProperties);
        // key-/truststore paths and passwords are not in properties:
        assertThrows(IllegalStateException.class, () -> _domain.createCuratorFramework());
    }

    @Test
    public void testConnectionEstablished() throws Exception {
        _domainProperties.setProperty("dcache.zookeeper.connection", "localhost:" + tlsPort);
        _domainProperties.setProperty("dcache.zookeeper.tls.key", curatorKeyPath);
        _domainProperties.setProperty("dcache.zookeeper.tls.cert", curatorCertPath);
        _domainProperties.setProperty("dcache.zookeeper.tls.capath", caPath);
        _domain = new Domain(DOMAIN_NAME, _domainProperties);
        CuratorFramework curator = _domain.createCuratorFramework();
        curator.start();
        boolean connected = curator.blockUntilConnected(2, TimeUnit.SECONDS);
        assertTrue(connected);
    }

    /*
     * SUPPORT METHODS
     */
    private void assertDomainServicesCount(int expectedCount) {
        assertEquals(expectedCount, _domain.getServices().size());
    }

    private void assertDomainPropertyValue(String key, String expectedValue) {
        assertEquals(expectedValue, _domain.properties().getValue(key));
    }

    @After
    public void tearDown() throws Exception {
        if (zkTestServer != null) {
            zkTestServer.close();
        }
    }
}
