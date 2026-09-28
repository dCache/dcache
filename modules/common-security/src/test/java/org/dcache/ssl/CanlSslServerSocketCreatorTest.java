package org.dcache.ssl;

import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CanlSslServerSocketCreatorTest {

    private SSLContext serverContext;
    private X509Certificate caCert;
    private X509Certificate validClientCert;
    private PrivateKey validClientKey;
    private X509Certificate untrustedClientCert;
    private PrivateKey untrustedClientKey;

    @BeforeClass
    public static void setupClass() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Before
    public void setup() throws Exception {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("ECDSA", "BC");
        keyGen.initialize(new ECGenParameterSpec("secp256r1"), new SecureRandom());

        KeyPair caKp = keyGen.generateKeyPair();
        X500Name caName = new X500Name("CN=Test-CA, O=dCache.org");
        caCert = buildCert(caName, caName, BigInteger.ONE, caKp.getPublic(), caKp, true);

        KeyPair serverKp = keyGen.generateKeyPair();
        X509Certificate serverCert = buildCert(caName, new X500Name("CN=localhost, O=dCache.org"),
              BigInteger.TWO, serverKp.getPublic(), caKp, false);
        serverContext = buildContext(serverKp.getPrivate(), serverCert, caCert);

        KeyPair validClientKp = keyGen.generateKeyPair();
        validClientKey = validClientKp.getPrivate();
        validClientCert = buildCert(caName, new X500Name("CN=valid-client, O=dCache.org"),
              BigInteger.valueOf(3), validClientKp.getPublic(), caKp, false);

        KeyPair untrustedCaKp = keyGen.generateKeyPair();
        X500Name untrustedCaName = new X500Name("CN=Untrusted-CA, O=evil.org");
        KeyPair untrustedClientKp = keyGen.generateKeyPair();
        untrustedClientKey = untrustedClientKp.getPrivate();
        untrustedClientCert = buildCert(untrustedCaName, new X500Name("CN=evil-client, O=evil.org"),
              BigInteger.TWO, untrustedClientKp.getPublic(), untrustedCaKp, false);
    }

    @Test
    public void connectWithValidCertSucceeds() throws Exception {
        assertConnects(buildContext(validClientKey, validClientCert, caCert));
    }

    @Test
    public void connectWithNoCertFails() throws Exception {
        assertRejected(buildContext(null, null, caCert));
    }

    @Test
    public void connectWithUntrustedCertFails() throws Exception {
        assertRejected(buildContext(untrustedClientKey, untrustedClientCert, caCert));
    }


    private void assertConnects(SSLContext clientContext) throws Exception {
        try (ServerSocket server = createServer()) {
            acceptInBackground(server);
            // passes if it does not throw
            try (SSLSocket client = connect(server.getLocalPort(), clientContext)) {
                client.startHandshake();
            }
        }
    }

    private void assertRejected(SSLContext clientContext) throws Exception {
        try (ServerSocket server = createServer()) {
            acceptInBackground(server);
            try (SSLSocket client = connect(server.getLocalPort(), clientContext)) {
                assertThrows(IOException.class, client::startHandshake);
            }
        }
    }


    private ServerSocket createServer() throws Exception {
        return new CanlSslServerSocketCreator(serverContext, true).createServerSocket(0);
    }

    private void acceptInBackground(ServerSocket server) {
        Executors.newSingleThreadExecutor().submit(() -> {
            try (Socket s = server.accept()) {
                ((SSLSocket) s).startHandshake();
            } catch (SSLException ignored) {}
            return null;
        });
    }

    private SSLSocket connect(int port, SSLContext context) throws Exception {
        SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket("localhost", port);
        socket.setEnabledProtocols(new String[]{"TLSv1.2"});
        return socket;
    }

    private SSLContext buildContext(PrivateKey key, X509Certificate cert,
          X509Certificate trustedCa) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        if (key != null) {
            ks.setKeyEntry("identity", key, new char[0], new Certificate[]{cert});
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, new char[0]);

        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        ts.setCertificateEntry("ca", trustedCa);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return context;
    }

    private X509Certificate buildCert(X500Name issuer, X500Name subject, BigInteger serial,
                                      PublicKey pubKey, KeyPair signerKeyPair, boolean isCA) throws Exception {
        long now = System.currentTimeMillis();
        X509v3CertificateBuilder builder = new X509v3CertificateBuilder(
                issuer, serial,
                new Date(now), new Date(now + TimeUnit.DAYS.toMillis(1)),
                subject, SubjectPublicKeyInfo.getInstance(pubKey.getEncoded()));
        if (isCA) {
            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        }
        return new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withECDSA")
                        .build(signerKeyPair.getPrivate())));
    }
}
