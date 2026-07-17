package org.dcache.boot;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import eu.emi.security.authn.x509.impl.CertificateUtils;

import java.io.File;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

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

    private X509Certificate caCert;
    private X509Certificate curatorCert;
    private PrivateKey curatorPrivateKey;

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
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("ECDSA", "BC");
        kpg.initialize(new ECGenParameterSpec("secp256r1"), new SecureRandom());

        KeyPair caKeyPair = kpg.generateKeyPair();
        X500Name caName = new X500Name("CN=ZooKeeper-Test-CA");
        caCert = buildCert(caName, caName, BigInteger.ONE, caKeyPair.getPublic(), caKeyPair, true);

        KeyPair zkKeyPair = kpg.generateKeyPair();
        X509Certificate zkCert = buildCert(caName, new X500Name("CN=localhost"), BigInteger.TWO, zkKeyPair.getPublic(), caKeyPair, false);

        KeyPair curatorKeyPair = kpg.generateKeyPair();
        curatorCert = buildCert(caName, new X500Name("CN=dcache-client"), BigInteger.valueOf(3), curatorKeyPair.getPublic(), caKeyPair, false);
        curatorPrivateKey = curatorKeyPair.getPrivate();

        try (OutputStream out = Files.newOutputStream(tempCaFile.toPath(), CREATE, TRUNCATE_EXISTING, WRITE)) {
            CertificateUtils.saveCertificate(out, caCert, CertificateUtils.Encoding.PEM);
        }

        try (OutputStream out = Files.newOutputStream(tempZkCombined.toPath(), CREATE, TRUNCATE_EXISTING, WRITE)) {
            CertificateUtils.saveCertificate(out, zkCert, CertificateUtils.Encoding.PEM);
            CertificateUtils.savePrivateKey(out, zkKeyPair.getPrivate(), CertificateUtils.Encoding.PEM, null, null);
        }

        try (OutputStream keyOut = Files.newOutputStream(tempCuratorKey.toPath(), CREATE, TRUNCATE_EXISTING, WRITE);
             OutputStream certOut = Files.newOutputStream(tempCuratorCert.toPath(), CREATE, TRUNCATE_EXISTING, WRITE)) {
            CertificateUtils.savePrivateKey(keyOut, curatorPrivateKey, CertificateUtils.Encoding.PEM, null, null);
            CertificateUtils.saveCertificate(certOut, curatorCert, CertificateUtils.Encoding.PEM);
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
        // createCuratorFramework() sets Domain._sslContextFactory via CanlContextFactory, which
        // needs the CA path to be a directory but does not require it to be populated.
        // Replace it with a standard Java SSLContext built directly from the in-memory certs.
        Domain._sslContextFactory = () -> buildSSLContext();
        curator.start();
        boolean connected = curator.blockUntilConnected(2, TimeUnit.SECONDS);
        assertTrue(connected);
    }

    private SSLContext buildSSLContext() throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("client", curatorPrivateKey, new char[0], new Certificate[]{curatorCert});
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, new char[0]);

        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", caCert);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return ctx;
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
        for (File f : new File[]{tempCaFile, tempZkCombined, tempCuratorKey, tempCuratorCert}) {
            if (f != null) {
                f.delete();
            }
        }
        if (tempCaDir != null) {
            File[] children = tempCaDir.listFiles();
            if (children != null) {
                for (File child : children) {
                    child.delete();
                }
            }
            tempCaDir.delete();
        }
    }
}
