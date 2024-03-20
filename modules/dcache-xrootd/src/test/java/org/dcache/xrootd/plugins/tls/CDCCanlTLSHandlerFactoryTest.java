package org.dcache.xrootd.plugins.tls;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.dcache.xrootd.plugins.tls.CDCCanlTLSHandlerFactory.*;
import static org.dcache.xrootd.plugins.tls.CDCCanlTLSHandlerFactory.OCSP_MODE;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CDCCanlTLSHandlerFactoryTest {


    // whatever slow test should run
    private static final boolean RUN_SLOW_TESTS = System.getProperty("run.slow.tests") != null;

    @BeforeClass
    public static void setupClass() {
        Security.addProvider(new BouncyCastleProvider());
    }
    private File keyFile;
    private File certFile;
    private File ca;

    private Properties configProperties;

    @Before
    public void setUp() throws Exception {
        keyFile = File.createTempFile("hostkey-", ".pem");
        certFile = File.createTempFile("hostcert-", ".pem");
        ca = Files.createTempDirectory("ca-").toFile();

        configProperties = new Properties();
        configProperties.setProperty(SERVICE_KEY, keyFile.getAbsolutePath());
        configProperties.setProperty(SERVICE_CERT, certFile.getAbsolutePath());
        configProperties.setProperty(SERVICE_CACERTS, ca.getAbsolutePath());
        configProperties.setProperty(NAMESPACE_MODE, "IGNORE");
        configProperties.setProperty(OCSP_MODE, "IGNORE");
        configProperties.setProperty(CRL_MODE, "IGNORE");
    }

    @After
    public void tearDown() throws Exception {
        keyFile.delete();
        certFile.delete();
        ca.delete();
    }

    @Test
    public void shouldReturnCachedContextIfFilesAreNotChanged() throws Exception {

        assumeTrue("Skip slow running test", RUN_SLOW_TESTS);

        generateSelfSignedCert();

        var contextSupplierFactory = new CDCCanlTLSHandlerFactory();
        var contextSupplier = contextSupplierFactory.buildContextSupplier(configProperties);

        var c1 = contextSupplier.get();

        TimeUnit.SECONDS.sleep(62); // 1 min is hard coded in org.dcache.ssl.CanlContextFactory.Builder.Builder

        var c2 = contextSupplier.get();
        assertSame(c1, c2);
    }

    @Test
    public void shouldReturnNewContextIfFilesChanged() throws Exception {

        assumeTrue("Skip slow running test", RUN_SLOW_TESTS);

        generateSelfSignedCert();
        var contextSupplierFactory = new CDCCanlTLSHandlerFactory();
        var contextSupplier = contextSupplierFactory.buildContextSupplier(configProperties);

        var c1 = contextSupplier.get();

        TimeUnit.SECONDS.sleep(62); // 1 min is hard coded in org.dcache.ssl.CanlContextFactory.Builder.Builder

        generateSelfSignedCert();
        var c2 = contextSupplier.get();

        assertNotSame(c1, c2);
    }

    // ensue that files are re-read
    @Test//(expected = FileNotFoundException.class)
    public void shouldFailToCreateContextIfFilesRemoved() throws Exception {

        assumeTrue("Skip slow running test", RUN_SLOW_TESTS);

        generateSelfSignedCert();
        var contextSupplierFactory = new CDCCanlTLSHandlerFactory();
        var contextSupplier = contextSupplierFactory.buildContextSupplier(configProperties);

        contextSupplier.get();

        TimeUnit.SECONDS.sleep(62); // 1 min is hard coded in org.dcache.ssl.CanlContextFactory.Builder.Builder

        keyFile.delete();
        certFile.delete();

        var t = assertThrows(RuntimeException.class, contextSupplier::get);
        assertThat(t.getCause(), new org.hamcrest.core.IsInstanceOf(FileNotFoundException.class));
    }

    private void generateSelfSignedCert()
            throws GeneralSecurityException, OperatorCreationException, IOException {

        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", "BC");
        keyPairGenerator.initialize(2048, new SecureRandom());
        KeyPair keyPair = keyPairGenerator.generateKeyPair();

        long notBefore = System.currentTimeMillis();
        long notAfter = notBefore + TimeUnit.DAYS.toMillis(1);

        X500Name subjectDN = new X500Name("CN=localhost, O=dCache.org");
        // explicit self-signed certificate
        X500Name issuerDN = subjectDN;

        SubjectPublicKeyInfo subjectPublicKeyInfo =
                SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());

        X509v3CertificateBuilder certificateBuilder = new X509v3CertificateBuilder(issuerDN,
                BigInteger.ONE,
                new Date(notBefore),
                new Date(notAfter), subjectDN,
                subjectPublicKeyInfo);

        String signatureAlgorithm = "SHA256WithRSA";

        // sign with own key
        ContentSigner contentSigner = new JcaContentSignerBuilder(signatureAlgorithm)
                .build(keyPair.getPrivate());

        X509CertificateHolder certificateHolder = certificateBuilder.build(contentSigner);
        var cert = new JcaX509CertificateConverter().getCertificate(certificateHolder);

        try (OutputStream certOut = Files.newOutputStream(
                certFile.toPath(), CREATE, TRUNCATE_EXISTING,
                WRITE); OutputStream keyOut = Files.newOutputStream(keyFile.toPath(), CREATE,
                TRUNCATE_EXISTING, WRITE)) {

            CertificateUtils.saveCertificate(certOut, cert, CertificateUtils.Encoding.PEM);
            CertificateUtils.savePrivateKey(keyOut, keyPair.getPrivate(), CertificateUtils.Encoding.PEM, null, null);
        }
    }

}

