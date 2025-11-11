/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 - 2025 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.ssl;

import static eu.emi.security.authn.x509.ValidationErrorCategory.CRL;
import static eu.emi.security.authn.x509.ValidationErrorCategory.NAMESPACE;
import static eu.emi.security.authn.x509.ValidationErrorCategory.NAME_CONSTRAINT;
import static eu.emi.security.authn.x509.ValidationErrorCategory.OCSP;
import static eu.emi.security.authn.x509.ValidationErrorCategory.X509_BASIC;
import static eu.emi.security.authn.x509.ValidationErrorCategory.X509_CHAIN;
import static org.dcache.util.Callables.memoizeFromFiles;
import static org.dcache.util.Callables.memoizeWithExpiration;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import eu.emi.security.authn.x509.OCSPParametes;
import eu.emi.security.authn.x509.ProxySupport;
import eu.emi.security.authn.x509.RevocationParameters;
import eu.emi.security.authn.x509.StoreUpdateListener;
import eu.emi.security.authn.x509.ValidationError;
import eu.emi.security.authn.x509.ValidationErrorCategory;
import eu.emi.security.authn.x509.X509CertChainValidator;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.helpers.AbstractDelegatingX509Credential;
import eu.emi.security.authn.x509.helpers.AbstractX509Credential;
import eu.emi.security.authn.x509.helpers.KeyStoreHelper;
import eu.emi.security.authn.x509.helpers.PasswordSupplier;
import eu.emi.security.authn.x509.helpers.ssl.SSLTrustManager;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.impl.KeystoreCredential;
import eu.emi.security.authn.x509.impl.OpensslCertChainValidator;
import eu.emi.security.authn.x509.impl.ValidatorParams;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.dcache.util.CachingCertificateValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SslContextFactory based on the CANL library. Uses the builder pattern to create immutable
 * instances.
 * <p/>
 * <p>
 * Implements the SslContextFactory which allows specifying either Java or Native (OpenSSL) as
 * implementation.
 */
public class CanlContextFactory implements SslContextFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanlContextFactory.class);

    private static final EnumSet<ValidationErrorCategory> VALIDATION_ERRORS_TO_LOG =
          EnumSet.of(NAMESPACE, X509_BASIC, X509_CHAIN, NAME_CONSTRAINT, CRL, OCSP);

    private final SecureRandom secureRandom = new SecureRandom();
    private final TrustManager[] trustManagers;
    private final boolean startTls;

    private static final AutoCloseable NOOP = new AutoCloseable() {
        @Override
        public void close() throws Exception {
        }
    };

    protected CanlContextFactory(boolean startTls, TrustManager... trustManagers) {
        this.startTls = startTls;
        this.trustManagers = trustManagers;
    }

    public static CanlContextFactory createDefault() throws IOException {
        return new Builder().build();
    }

    public static Builder custom() {
        return new Builder();
    }

    public TrustManager[] getTrustManagers() {
        return trustManagers;
    }

    @Override
    public <T> T getContext(Class<T> type, X509Credential credential)
          throws GeneralSecurityException {
        if (type.isAssignableFrom(SSLContext.class)) {
            return (T) getJavaSSLContext(credential);
        } else if (type.isAssignableFrom(SslContext.class)) {
            return (T) getNettySslContext(credential);
        }

        throw new GeneralSecurityException("cannot get SSL context of type " + type);
    }

    private SSLContext getJavaSSLContext(X509Credential credential)
          throws GeneralSecurityException {
        KeyManager[] keyManagers;
        if (credential == null) {
            keyManagers = null;
        } else {
            keyManagers = new KeyManager[1];
            keyManagers[0] = credential.getKeyManager();
        }
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagers, trustManagers, secureRandom);
        return context;
    }

    private SslContext getNettySslContext(X509Credential credential)
          throws GeneralSecurityException {
        KeyManager keyManager = credential == null ? null : credential.getKeyManager();
        SslContextBuilder builder = startTls ? SslContextBuilder.forServer(keyManager)
              : SslContextBuilder.forClient();
        try {
            return builder.trustManager(trustManagers[0]).startTls(startTls).build();
        } catch (SSLException e) {
            throw new GeneralSecurityException(
                  "Could not get Netty SSL context: " + e.getMessage());
        }
    }

    public static class Builder {

        private Path certificateAuthorityPath = FileSystems.getDefault()
              .getPath("/etc/grid-security/certificates");
        private NamespaceCheckingMode namespaceMode = NamespaceCheckingMode.EUGRIDPMA_GLOBUS;
        private CrlCheckingMode crlCheckingMode = CrlCheckingMode.IF_VALID;
        private OCSPCheckingMode ocspCheckingMode = OCSPCheckingMode.IF_AVAILABLE;
        private long certificateAuthorityUpdateInterval = 600000;
        private boolean lazyMode = true;
        private Path keyPath = FileSystems.getDefault().getPath("/etc/grid-security/hostkey.pem");
        private Path certificatePath = FileSystems.getDefault()
              .getPath("/etc/grid-security/hostcert.pem");
        private long credentialUpdateInterval = 1;
        private TimeUnit credentialUpdateIntervalUnit = TimeUnit.MINUTES;
        private Supplier<AutoCloseable> loggingContextSupplier = () -> NOOP;
        private long validationCacheLifetime = 300000;
        private boolean startTls = true; // default/server mode

        private Builder() {
        }

        public Builder startTls(boolean startTls) {
            this.startTls = startTls;
            return this;
        }

        public Builder withCertificateAuthorityPath(Path certificateAuthorityPath) {
            this.certificateAuthorityPath = certificateAuthorityPath;
            return this;
        }

        public Builder withCertificateAuthorityPath(String certificateAuthorityPath) {
            return withCertificateAuthorityPath(
                  FileSystems.getDefault().getPath(certificateAuthorityPath));
        }

        public Builder withCertificateAuthorityUpdateInterval(long interval) {
            this.certificateAuthorityUpdateInterval = interval;
            return this;
        }

        public Builder withCertificateAuthorityUpdateInterval(long interval, TimeUnit unit) {
            this.certificateAuthorityUpdateInterval = unit.toMillis(interval);
            return this;
        }

        public Builder withCrlCheckingMode(CrlCheckingMode crlCheckingMode) {
            this.crlCheckingMode = crlCheckingMode;
            return this;
        }

        public Builder withOcspCheckingMode(OCSPCheckingMode ocspCheckingMode) {
            this.ocspCheckingMode = ocspCheckingMode;
            return this;
        }

        public Builder withNamespaceMode(NamespaceCheckingMode namespaceMode) {
            this.namespaceMode = namespaceMode;
            return this;
        }

        public Builder withLazy(boolean lazyMode) {
            this.lazyMode = lazyMode;
            return this;
        }

        public Builder withKeyPath(Path keyPath) {
            this.keyPath = keyPath;
            return this;
        }

        public Builder withCertificatePath(Path certificatePath) {
            this.certificatePath = certificatePath;
            return this;
        }

        public Builder withCredentialUpdateInterval(long duration, TimeUnit unit) {
            this.credentialUpdateInterval = duration;
            this.credentialUpdateIntervalUnit = unit;
            return this;
        }

        public Builder withLoggingContext(Supplier<AutoCloseable> contextSupplier) {
            this.loggingContextSupplier = contextSupplier;
            return this;
        }

        public Builder withValidationCacheLifetime(long millis) {
            this.validationCacheLifetime = millis;
            return this;
        }

        public Builder withValidationCacheLifetime(long duration, TimeUnit unit) {
            this.validationCacheLifetime = unit.toMillis(duration);
            return this;
        }

        public CanlContextFactory build() throws IOException {
            File caPath = new File(certificateAuthorityPath.toString());
            if (!caPath.isDirectory()) {
                throw new FileNotFoundException(caPath +
                      " is missing: HTTPS requires the certificate authority CRLs");
            }

            OCSPParametes ocspParameters = new OCSPParametes(ocspCheckingMode);
            ValidatorParams validatorParams =
                  new ValidatorParams(new RevocationParameters(crlCheckingMode, ocspParameters),
                        ProxySupport.ALLOW);
            X509CertChainValidator v =
                  new CachingCertificateValidator(
                        new OpensslCertChainValidator(certificateAuthorityPath.toString(), true,
                              namespaceMode,
                              certificateAuthorityUpdateInterval,
                              validatorParams, lazyMode),
                        validationCacheLifetime);
            v.addUpdateListener(new StoreUpdateListener() {
                @Override
                public void loadingNotification(String location, String type, Severity level,
                      Exception cause) {
                    try (AutoCloseable ignored = loggingContextSupplier.get()) {
                        switch (level) {
                            case ERROR:
                                if (cause != null) {
                                    LOGGER.error("Error loading {} from {}: {}", type, location,
                                          cause.getMessage());
                                } else {
                                    LOGGER.error("Error loading {} from {}.", type, location);
                                }
                                break;
                            case WARNING:
                                if (cause != null) {
                                    LOGGER.warn("Problem loading {} from {}: {}", type, location,
                                          cause.getMessage());
                                } else {
                                    LOGGER.warn("Problem loading {} from {}.", type, location);
                                }
                                break;
                            case NOTIFICATION:
                                LOGGER.debug("Reloaded {} from {}.", type, location);
                                break;
                        }
                    } catch (Exception e) {
                        Throwables.throwIfUnchecked(e);
                        throw new RuntimeException(e);
                    }
                }
            });
            v.addValidationListener((ValidationError error) -> {
                if (VALIDATION_ERRORS_TO_LOG.contains(error.getErrorCategory())) {
                    X509Certificate[] chain = error.getChain();
                    String subject =
                          (chain != null && chain.length > 0) ? chain[0].getSubjectX500Principal()
                                .getName() : "";
                    LOGGER.warn("The peer's certificate with DN {} was rejected: {}", subject,
                          error);
                }
                return false;
            });
            return new CanlContextFactory(startTls, new SSLTrustManager(v));
        }

        public <T> Callable<T> buildWithCaching(Class<T> contextType) throws Exception {
            final CanlContextFactory factory = build();
            /*
             * PEMCredential does not consistently support keyPasswd being null
             * https://github.com/eu-emi/canl-java/issues/114
             */
            Callable newContext = () -> {
                PEMCredential0 credential
                      = new PEMCredential0(keyPath.toString(), certificatePath.toString(), new char[]{});
                LOGGER.info("Reloading host credential {} {}", certificatePath, keyPath);
                return factory.getContext(contextType, credential);
            };

            return (Callable<T>) memoizeWithExpiration(memoizeFromFiles(newContext,
                        keyPath,
                        certificatePath),
                  credentialUpdateInterval,
                  credentialUpdateIntervalUnit);
        }
    }

    /*
        Based on CANL PEMCredential and KeyAndCertCredential classes

     */

    // PEMCredential0 is a copy of PEMCredential that uses custom KeyAndCertCredential

    private static class PEMCredential0 extends AbstractDelegatingX509Credential {

        public PEMCredential0(String keyPath, String certificatePath, char[] keyPasswd) throws IOException, CertificateException, KeyStoreException {
            this(new FileInputStream(keyPath), new FileInputStream(certificatePath), keyPasswd);
        }

        public PEMCredential0(InputStream privateKeyStream, InputStream certificateStream, char[] keyPasswd)
                throws IOException, KeyStoreException, CertificateException {
            this(privateKeyStream, certificateStream, CertificateUtils.getPF(keyPasswd));
        }

        public PEMCredential0(InputStream privateKeyStream, InputStream certificateStream, PasswordSupplier pf)
                throws IOException, KeyStoreException {
            X509Certificate[] chain = CertificateUtils.loadCertificateChain(
                    certificateStream, CertificateUtils.Encoding.PEM);
            PrivateKey pk = CertificateUtils.loadPEMPrivateKey(privateKeyStream, pf);
            privateKeyStream.close();
            delegate = new KeyAndCertCredential0(pk, chain);
        }
    }

    /**
     * KeyAndCertCredential0 is a copy of KeyAndCertCredential that support EC and ECDSA keys.
     */
    private static class KeyAndCertCredential0 extends AbstractX509Credential {

        private static final byte[] TEST = new byte[]{1, 2, 3, 4, 100};

        /**
         * Creates a new instance from the provided key and certificates.
         *
         * @param privateKey       private key to be placed in this {@link X509Credential}'s KeyStore
         * @param certificateChain certificates to be placed in this {@link X509Credential}'s KeyStore.
         *                         those certificates must match the provided privateKey. The user's certificate is assumed
         *                         to be the first entry in the chain.
         * @throws KeyStoreException if private key is invalid or doesn't match the certificate.
         */
        public KeyAndCertCredential0(PrivateKey privateKey, X509Certificate[] certificateChain)
                throws KeyStoreException {
            try {
                ks = KeyStoreHelper.getInstanceForCredential("JKS");
            } catch (KeyStoreException e) {
                throw new RuntimeException("Can't create JKS KeyStore - JDK is misconfgured?", e);
            }

            try {
                ks.load(null);
            } catch (Exception e) {
                throw new RuntimeException("Can't init JKS KeyStore - JDK is misconfgured?", e);
            }

            PublicKey pubKey = certificateChain[0].getPublicKey();
            String pubKeyAlgorithm = pubKey.getAlgorithm();
            // REVISIT: BouncyCastle uses "ECDSA" as the private key algorithm and "EC" as the public key algorithm names for elliptic curve keys.
            if (!privateKey.getAlgorithm().equals(pubKeyAlgorithm) && !(privateKey.getAlgorithm().equals("ECDSA") && pubKeyAlgorithm.equals("EC")))
                throw new KeyStoreException("Private and public keys are not matching: different algorithms: "
                        + privateKey.getAlgorithm() + " vs. " + pubKeyAlgorithm);

            switch (pubKeyAlgorithm) {
                case "DSA":
                    if (!checkKeysViaSignature("SHA1withDSA", privateKey, pubKey))
                        throw new KeyStoreException("Private and public keys are not matching: DSA");
                    break;
                case "RSA":
                    RSAPublicKey rpub = (RSAPublicKey) pubKey;
                    RSAPrivateKey rpriv = (RSAPrivateKey) privateKey;
                    if (!rpub.getModulus().equals(rpriv.getModulus()))
                        throw new KeyStoreException("Private and public keys are not matching: RSA parameters");
                    break;
                case "GOST3410":
                    if (!checkKeysViaSignature("GOST3411withGOST3410", privateKey, pubKey))
                        throw new KeyStoreException("Private and public keys are not matching: GOST 34.10");
                    break;
                case "ECGOST3410":
                    if (!checkKeysViaSignature("GOST3411withECGOST3410", privateKey, pubKey))
                        throw new KeyStoreException("Private and public keys are not matching: EC GOST 34.10");
                    break;
                case "ECDSA":
                    if (!checkKeysViaSignature("SHA1withECDSA", privateKey, pubKey))
                        throw new KeyStoreException("Private and public keys are not matching: EC DSA");
                    break;
            }

            ks.setKeyEntry(KeystoreCredential.ALIAS, privateKey,
                    KeystoreCredential.KEY_PASSWD, certificateChain);
        }

        private static boolean checkKeysViaSignature(String alg, PrivateKey privKey, PublicKey pubKey) throws KeyStoreException {
            try {
                Signature s = Signature.getInstance(alg);
                s.initSign(privKey);
                s.update(TEST);
                byte[] signature = s.sign();
                Signature s2 = Signature.getInstance(alg);
                s2.initVerify(pubKey);
                s2.update(TEST);
                return s2.verify(signature);
            } catch (InvalidKeyException e) {
                throw new KeyStoreException("Invalid key when checking key match", e);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Bug: BC provider not available in checkKeysMatching()", e);
            } catch (SignatureException e) {
                throw new RuntimeException("Bug: can't sign/verify test data", e);
            }
        }
    }
}
