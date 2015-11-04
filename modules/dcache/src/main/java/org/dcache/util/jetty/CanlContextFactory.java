/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.util.jetty;

import com.google.common.base.Throwables;
import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import eu.emi.security.authn.x509.impl.PEMCredential;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;

import java.io.File;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CRL;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CDC;

import org.dcache.gsi.GsiFrameEngine;
import org.dcache.gsi.KeyPairCache;
import org.dcache.gsi.ServerGsiEngine;
import org.dcache.util.Callables;

/**
 * Specialized SSLContext factory that uses CANL for certificate handling.
 *
 * Can optionally create GSIEngine wrappers for SSLEngine to support GSI delegation. Should be
 * combined with GsiRequestCustomizer to add the delegated credentials to the HttpServletRequest.
 */
public class CanlContextFactory extends SslContextFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CanlContextFactory.class);

    private Path certificatePath;
    private Path keyPath;
    private boolean isGsiEnabled;
    private boolean isUsingLegacyClose;
    private CertificateFactory cf;
    private Path certificateAuthorityPath;
    private NamespaceCheckingMode namespaceMode = NamespaceCheckingMode.EUGRIDPMA_GLOBUS;
    private CrlCheckingMode crlCheckingMode = CrlCheckingMode.IF_VALID;
    private OCSPCheckingMode ocspCheckingMode = OCSPCheckingMode.IF_AVAILABLE;
    private long certificateAuthorityUpdateInterval = 600000;
    private long credentialUpdateInterval = 60000;

    private KeyPairCache keyPairCache;

    private Callable<SslContextFactory> delegate;

    public File getCertificatePath()
    {
        return certificatePath.toFile();
    }

    public void setCertificatePath(File certificatePath)
    {
        this.certificatePath = certificatePath.toPath();
    }

    public File getKeyPath()
    {
        return keyPath.toFile();
    }

    public void setKeyPath(File keyPath)
    {
        this.keyPath = keyPath.toPath();
    }

    public File getCertificateAuthorityPath()
    {
        return certificateAuthorityPath.toFile();
    }

    public void setCertificateAuthorityPath(File certificateAuthorityPath)
    {
        this.certificateAuthorityPath = certificateAuthorityPath.toPath();
    }

    public boolean isGsiEnabled()
    {
        return isGsiEnabled;
    }

    public void setGsiEnabled(boolean value)
    {
        isGsiEnabled = value;
    }

    public boolean isUsingLegacyClose()
    {
        return isUsingLegacyClose;
    }

    public void setUsingLegacyClose(boolean usingLegacyClose)
    {
        this.isUsingLegacyClose = usingLegacyClose;
    }

    public long getCertificateAuthorityUpdateInterval()
    {
        return certificateAuthorityUpdateInterval;
    }

    public void setCertificateAuthorityUpdateInterval(long interval)
    {
        this.certificateAuthorityUpdateInterval = interval;
    }

    public long getCredentialUpdateInterval()
    {
        return credentialUpdateInterval;
    }

    public void setCredentialUpdateInterval(long interval)
    {
        this.credentialUpdateInterval = interval;
    }

    public CrlCheckingMode getCrlCheckingMode()
    {
        return crlCheckingMode;
    }

    public void setCrlCheckingMode(CrlCheckingMode crlCheckingMode)
    {
        this.crlCheckingMode = crlCheckingMode;
    }

    public OCSPCheckingMode getOcspCheckingMode()
    {
        return ocspCheckingMode;
    }

    public void setOcspCheckingMode(OCSPCheckingMode ocspCheckingMode)
    {
        this.ocspCheckingMode = ocspCheckingMode;
    }

    public NamespaceCheckingMode getNamespaceMode()
    {
        return namespaceMode;
    }

    public void setNamespaceMode(NamespaceCheckingMode namespaceMode)
    {
        this.namespaceMode = namespaceMode;
    }

    public KeyPairCache getKeyPairCache()
    {
        return keyPairCache;
    }

    public void setKeyPairCache(KeyPairCache keyPairCache)
    {
        this.keyPairCache = keyPairCache;
    }

    @Override
    protected void doStart() throws Exception
    {
        cf = CertificateFactory.getInstance("X.509");
        delegate = Callables.memoizeWithExpiration(
                Callables.memoizeFromFiles(this::createDelegate, keyPath, certificatePath),
                credentialUpdateInterval, TimeUnit.MILLISECONDS);
        delegate.call(); // Fail fast
    }

    /**
     * Creates an SslContextFactory to which SSLEngine creation can be delegated.
     *
     * The reason to create a delegate is that SslContextFactory doesn't allow the SSLContext
     * to be recreated once initialized. Thus the only means of reloading the host key is
     * to recreate the entire factory.
     */
    private SslContextFactory createDelegate() throws Exception
    {
        SslContextFactory factory = new SslContextFactory()
        {
            private PEMCredential serverCredential =
                    new PEMCredential(keyPath.toString(), certificatePath.toString(), null);

            @Override
            protected void doStart() throws Exception
            {
                super.setCertAlias(CanlContextFactory.this.getCertAlias());
                super.setCipherComparator(CanlContextFactory.this.getCipherComparator());
                super.setExcludeCipherSuites(CanlContextFactory.this.getExcludeCipherSuites());
                super.setExcludeProtocols(CanlContextFactory.this.getExcludeProtocols());
                super.setIncludeCipherSuites(CanlContextFactory.this.getIncludeCipherSuites());
                super.setIncludeProtocols(CanlContextFactory.this.getIncludeProtocols());
                super.setMaxCertPathLength(CanlContextFactory.this.getMaxCertPathLength());
                super.setProtocol(CanlContextFactory.this.getProtocol());
                super.setProvider(CanlContextFactory.this.getProvider());
                super.setRenegotiationAllowed(CanlContextFactory.this.isRenegotiationAllowed());
                super.setSecureRandomAlgorithm(CanlContextFactory.this.getSecureRandomAlgorithm());
                super.setSessionCachingEnabled(CanlContextFactory.this.isSessionCachingEnabled());
                super.setSslSessionCacheSize(CanlContextFactory.this.getSslSessionCacheSize());
                super.setSslSessionTimeout(CanlContextFactory.this.getSslSessionTimeout());
                super.setStopTimeout(CanlContextFactory.this.getStopTimeout());
                super.setUseCipherSuitesOrder(CanlContextFactory.this.isUseCipherSuitesOrder());
                super.setWantClientAuth(CanlContextFactory.this.getWantClientAuth());
                super.setNeedClientAuth(CanlContextFactory.this.getNeedClientAuth());
                super.setKeyStore(serverCredential.getKeyStore());
                super.doStart();
            }

            @Override
            protected KeyStore loadKeyStore(Resource resource) throws Exception
            {
                return null;
            }

            @Override
            protected KeyStore loadTrustStore(Resource resource) throws Exception
            {
                return null;
            }

            @Override
            protected Collection<? extends CRL> loadCRL(String crlPath) throws Exception
            {
                return null;
            }

            @Override
            protected KeyManager[] getKeyManagers(KeyStore keyStore) throws Exception
            {
                return new KeyManager[] { serverCredential.getKeyManager() };
            }

            @Override
            protected TrustManager[] getTrustManagers(KeyStore trustStore,
                                                      Collection<? extends CRL> crls) throws Exception
            {
                return org.dcache.ssl.CanlContextFactory.custom()
                        .withOcspCheckingMode(ocspCheckingMode)
                        .withCrlCheckingMode(crlCheckingMode)
                        .withNamespaceMode(namespaceMode)
                        .withCertificateAuthorityPath(certificateAuthorityPath)
                        .withCertificateAuthorityUpdateInterval(certificateAuthorityUpdateInterval)
                        .withLazy(false)
                        .withLoggingContext(new CDC()::restore)
                        .build()
                        .getTrustManagers();
            }
        };
        factory.start();
        return factory;
    }

    protected SSLEngine wrapEngine(SSLEngine engine)
    {
        if (isGsiEnabled) {
            ServerGsiEngine gsiEngine = new ServerGsiEngine(engine, cf);
            gsiEngine.setUsingLegacyClose(isUsingLegacyClose);
            gsiEngine.setKeyPairCache(keyPairCache);
            return new GsiFrameEngine(gsiEngine);
        } else {
            return engine;
        }
    }

    @Override
    public SSLEngine newSSLEngine()
    {
        try {
            return wrapEngine(delegate.call().newSSLEngine());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public SSLEngine newSSLEngine(String host, int port)
    {
        try {
            return wrapEngine(delegate.call().newSSLEngine(host, port));
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
