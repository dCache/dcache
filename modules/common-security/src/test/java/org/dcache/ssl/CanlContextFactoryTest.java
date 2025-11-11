package org.dcache.ssl;

import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import io.netty.handler.ssl.SslContext;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Security;
import java.security.spec.ECGenParameterSpec;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class CanlContextFactoryTest {

    private File keyFile;
    private File certFile;
    private File ca;


    @BeforeClass
    public static void setupClass() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Before
    public void setup() throws IOException, GeneralSecurityException, OperatorCreationException {
        keyFile = File.createTempFile("hostkey-", ".pem");
        certFile = File.createTempFile("hostcert-", ".pem");
        ca = Files.createTempDirectory("ca-").toFile();
        generateSelfSignedCert();
    }

    @Test
    public void testEcdsaKey() throws Exception {

        var builder = CanlContextFactory.custom()
                .withCertificatePath(certFile.toPath())
                .withKeyPath(keyFile.toPath())
                .withCertificateAuthorityPath(ca.toPath())
                .withCrlCheckingMode(CrlCheckingMode.REQUIRE)
                .withOcspCheckingMode(OCSPCheckingMode.IF_AVAILABLE)
                .withNamespaceMode(NamespaceCheckingMode.EUGRIDPMA_AND_GLOBUS)
                .withLazy(false)
                .buildWithCaching(SslContext.class);

        builder.call();
    }

    private void generateSelfSignedCert()
            throws GeneralSecurityException, OperatorCreationException, IOException {

        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("ECDSA", "BC");

        ECGenParameterSpec ecSpec = new ECGenParameterSpec("secp256r1");

        keyPairGenerator.initialize(ecSpec, new SecureRandom());
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

        String signatureAlgorithm = "SHA1withECDSA";

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