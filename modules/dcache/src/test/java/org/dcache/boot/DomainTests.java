package org.dcache.boot;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import eu.emi.security.authn.x509.helpers.trust.OpensslTruststoreHelper;
import eu.emi.security.authn.x509.impl.CertificateUtils;

import java.io.File;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.dcache.util.configuration.ConfigurationProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
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

    private File tempCaDir;
    private File tempCaFile;
    private File tempZkCombined;
    private File tempCuratorKey;
    private File tempCuratorCert;

    @BeforeClass
    public static void setUpClass() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Before
    public void setUp() throws Exception {
        tempCaDir = Files.createTempDirectory("ca-dir-").toFile();
        tempCaFile = File.createTempFile("ca-cert-", ".pem");
        tempZkCombined = File.createTempFile("zk-combined-", ".pem");
        tempCuratorKey = File.createTempFile("curator-key-", ".pem");
        tempCuratorCert = File.createTempFile("curator-cert-", ".pem");
        generateCertificates();

        caPath = tempCaDir.getAbsolutePath();
        curatorKeyPath = tempCuratorKey.getAbsolutePath();
        curatorCertPath = tempCuratorCert.getAbsolutePath();

        tlsPort = InstanceSpec.getRandomPort();
        HashMap<String, Object> zkProperties = new HashMap<>();
        zkProperties.put("clientPort", "0");
        zkProperties.put("secureClientPort", String.valueOf(tlsPort));
        zkProperties.put("serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");
        zkProperties.put("ssl.keyStore.location", tempZkCombined.getAbsolutePath());
        zkProperties.put("ssl.keyStore.type", "PEM");
        zkProperties.put("ssl.trustStore.location", tempCaFile.getAbsolutePath());
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

    private void generateCertificates() throws Exception {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("ECDSA", "BC");
        keyPairGenerator.initialize(new ECGenParameterSpec("secp256r1"), new SecureRandom());

        KeyPair caKeyPair = keyPairGenerator.generateKeyPair();
        X500Name caName = new X500Name("CN=ZooKeeper-Test-CA");
        X509Certificate caCert = buildCert(caName, caName, BigInteger.ONE, caKeyPair.getPublic(), caKeyPair, true);

        KeyPair zkKeyPair = keyPairGenerator.generateKeyPair();
        X509Certificate zkCert = buildCert(caName, new X500Name("CN=localhost"), BigInteger.TWO, zkKeyPair.getPublic(), caKeyPair, false);

        KeyPair curatorKeyPair = keyPairGenerator.generateKeyPair();
        X509Certificate curatorCert = buildCert(caName, new X500Name("CN=dcache-client"), BigInteger.valueOf(3), curatorKeyPair.getPublic(), caKeyPair, false);

        try (OutputStream out = Files.newOutputStream(tempCaFile.toPath(), CREATE, TRUNCATE_EXISTING, WRITE)) {
            CertificateUtils.saveCertificate(out, caCert, CertificateUtils.Encoding.PEM);
        }

        try (OutputStream out = Files.newOutputStream(tempZkCombined.toPath(), CREATE, TRUNCATE_EXISTING, WRITE)) {
            CertificateUtils.saveCertificate(out, zkCert, CertificateUtils.Encoding.PEM);
            CertificateUtils.savePrivateKey(out, zkKeyPair.getPrivate(), CertificateUtils.Encoding.PEM, null, null);
        }

        try (OutputStream keyOut = Files.newOutputStream(tempCuratorKey.toPath(), CREATE, TRUNCATE_EXISTING, WRITE);
             OutputStream certOut = Files.newOutputStream(tempCuratorCert.toPath(), CREATE, TRUNCATE_EXISTING, WRITE)) {
            CertificateUtils.savePrivateKey(keyOut, curatorKeyPair.getPrivate(), CertificateUtils.Encoding.PEM, null, null);
            CertificateUtils.saveCertificate(certOut, curatorCert, CertificateUtils.Encoding.PEM);
        }

        String caHash = OpensslTruststoreHelper.getOpenSSLCAHash(caCert.getSubjectX500Principal(), true);
        try (OutputStream out = Files.newOutputStream(new File(tempCaDir, caHash + ".0").toPath(), CREATE, WRITE)) {
            CertificateUtils.saveCertificate(out, caCert, CertificateUtils.Encoding.PEM);
        }
    }

    private X509Certificate buildCert(X500Name issuer, X500Name subject, BigInteger serial,
            PublicKey pubKey, KeyPair signerKeyPair, boolean isCA) throws Exception {
        long now = System.currentTimeMillis();
        var builder = new X509v3CertificateBuilder(
                issuer, serial,
                new Date(now), new Date(now + TimeUnit.DAYS.toMillis(1)),
                subject, SubjectPublicKeyInfo.getInstance(pubKey.getEncoded()));
        if (isCA) {
            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        }
        var holder = builder.build(new JcaContentSignerBuilder("SHA256withECDSA").build(signerKeyPair.getPrivate()));
        return new JcaX509CertificateConverter().getCertificate(holder);
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
        File[] certs = {tempCaFile, tempZkCombined, tempCuratorKey, tempCuratorCert};
        for (File c : certs) {
            if (c != null) {
                c.delete();
            }
        }
        if (tempCaDir != null) {
            File[] files = tempCaDir.listFiles();
            if (files != null) {
                for (File f : files) {
                    f.delete();
                }
            }
            tempCaDir.delete();
        }
    }
}
