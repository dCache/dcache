/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.gsi;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.globus.gsi.X509Credential;
import org.globus.gsi.jsse.GlobusSSLConfigurationException;
import org.globus.gsi.provider.GlobusProvider;
import org.globus.gsi.provider.GlobusTrustManagerFactoryParameters;
import org.globus.gsi.provider.KeyStoreParametersFactory;
import org.globus.gsi.provider.SigningPolicyStore;
import org.globus.gsi.proxy.ProxyPolicyHandler;
import org.globus.gsi.stores.ResourceCertStoreParameters;
import org.globus.gsi.stores.ResourceSigningPolicyStore;
import org.globus.gsi.stores.ResourceSigningPolicyStoreParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CRL;
import java.security.cert.CertStore;
import java.security.cert.CertificateFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Specialized SSLContext factory that uses Globus classes for certificate handling.
 *
 * Can optionally create GSIEngine wrappers for SSLEngine to support GSI delegation. Should be
 * combined with GsiRequestCustomizer to add the delegated credentials to the HttpServletRequest.
 */
public class GlobusContextFactory extends SslContextFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobusContextFactory.class);
    private String serverCertificatePath;
    private String serverKeyPath;
    private boolean rejectLimitProxy;
    private Map<String, ProxyPolicyHandler> proxyPolicyHandlers;
    private boolean isGsiEnabled;
    private boolean isUsingLegacyClose;
    private CertificateFactory cf;
    private String trustStorePath;

    public GlobusContextFactory()
    {
        setTrustStoreProvider(GlobusProvider.PROVIDER_NAME);
        setTrustStoreType(GlobusProvider.KEYSTORE_TYPE);
        setKeyStorePassword("password");
        setTrustManagerFactoryAlgorithm("GSI");
    }

    public String getServerCertificatePath()
    {
        return serverCertificatePath;
    }

    public void setServerCertificatePath(String serverCertificatePath)
    {
        checkNotStarted();
        this.serverCertificatePath = serverCertificatePath;
    }

    public String getServerKeyPath()
    {
        return serverKeyPath;
    }

    public void setServerKeyPath(String serverKeyPath)
    {
        checkNotStarted();
        this.serverKeyPath = serverKeyPath;
    }

    @Override
    public void setTrustStorePath(String trustStorePath)
    {
        /* https://bugs.eclipse.org/bugs/show_bug.cgi?id=476720 */
        this.trustStorePath = trustStorePath;
    }

    public boolean isRejectLimitProxy()
    {
        return rejectLimitProxy;
    }

    public void setRejectLimitProxy(boolean rejectLimitProxy)
    {
        this.rejectLimitProxy = rejectLimitProxy;
    }

    public Map<String, ProxyPolicyHandler> getProxyPolicyHandlers()
    {
        return proxyPolicyHandlers;
    }

    public void setProxyPolicyHandlers(Map<String, ProxyPolicyHandler> proxyPolicyHandlers)
    {
        this.proxyPolicyHandlers = proxyPolicyHandlers;
    }

    public void setEnableGsi(boolean value)
    {
        isGsiEnabled = value;
    }

    public boolean isGsiEnabled()
    {
        return isGsiEnabled;
    }

    public boolean isUsingLegacyClose()
    {
        return isUsingLegacyClose;
    }

    public void setUsingLegacyClose(boolean usingLegacyClose)
    {
        this.isUsingLegacyClose = usingLegacyClose;
    }

    @Override
    protected void doStart() throws Exception
    {
        KeyStore keyStore = loadKeyStore();
        KeyStore trustStore = loadTrustStore();
        TrustManager[] trustManagers = getTrustManagers(trustStore, Collections.<CRL>emptyList());
        KeyManager[] keyManagers = getKeyManagers(keyStore);
        String secureRandomAlgorithm = getSecureRandomAlgorithm();
        SecureRandom secureRandom = (secureRandomAlgorithm == null) ? null : SecureRandom.getInstance(secureRandomAlgorithm);

        String provider = getProvider();
        String protocol = getProtocol();
        SSLContext sslContext =
                (provider == null) ? SSLContext.getInstance(protocol) : SSLContext.getInstance(protocol, provider);
        try {
            sslContext.init(keyManagers, trustManagers, secureRandom);
        } catch (KeyManagementException e) {
            throw new GlobusSSLConfigurationException(e);
        }
        setSslContext(sslContext);

        cf = CertificateFactory.getInstance("X.509");

        _factory = new Factory(keyStore, trustStore, sslContext);

        if (LOGGER.isDebugEnabled())
        {
            SSLEngine engine = newSSLEngine();
            LOGGER.debug("Enabled Protocols {} of {}", Arrays.asList(engine.getEnabledProtocols()), Arrays.asList(engine.getSupportedProtocols()));
            LOGGER.debug("Enabled Ciphers   {} of {}", Arrays.asList(engine.getEnabledCipherSuites()), Arrays.asList(engine.getSupportedCipherSuites()));
        }
    }

    protected KeyStore loadTrustStore() throws Exception
    {
        final String caCertsPattern = trustStorePath + "/*.0";
        final KeyStore keyStore = KeyStore.getInstance(getTrustStoreType(), getTrustStoreProvider());
        keyStore.load(KeyStoreParametersFactory.createTrustStoreParameters(caCertsPattern));
        return keyStore;
    }

    protected KeyStore loadKeyStore() throws Exception
    {
        X509Credential cred = new X509Credential(getServerCertificatePath(), getServerKeyPath());
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setKeyEntry("default", cred.getPrivateKey(), "password".toCharArray(), cred.getCertificateChain());
        return keyStore;
    }

    protected CertStore loadCrlStore() throws Exception
    {
        String crlPattern = trustStorePath + "/*.r*";
        return CertStore.getInstance(GlobusProvider.CERTSTORE_TYPE, new ResourceCertStoreParameters(null, crlPattern));
    }

    protected ResourceSigningPolicyStore loadSigningPolicyStore() throws Exception
    {
        String sigPolPattern = trustStorePath + "/*.signing_policy";
        return new ResourceSigningPolicyStore(new ResourceSigningPolicyStoreParameters(sigPolPattern));
    }

    @Override
    protected TrustManager[] getTrustManagers(KeyStore trustStore, Collection<? extends CRL> crls) throws Exception
    {
        ManagerFactoryParameters parameters = getCertPathParameters(trustStore);
        TrustManagerFactory fact = TrustManagerFactory.getInstance(getTrustManagerFactoryAlgorithm());
        fact.init(parameters);
        return fact.getTrustManagers();
    }

    private ManagerFactoryParameters getCertPathParameters(KeyStore trustStore)
            throws Exception
    {
        GlobusTrustManagerFactoryParameters parameters;
        CertStore crlStore = loadCrlStore();
        SigningPolicyStore policyStore = loadSigningPolicyStore();
        if (proxyPolicyHandlers == null) {
            parameters = new GlobusTrustManagerFactoryParameters(
                    trustStore, crlStore, policyStore,
                    this.rejectLimitProxy);
        } else {
            parameters = new GlobusTrustManagerFactoryParameters(
                    trustStore, crlStore, policyStore,
                    this.rejectLimitProxy, proxyPolicyHandlers);
        }
        return parameters;
    }

    private SSLEngine wrapEngine(SSLEngine engine)
    {
        if (isGsiEnabled) {
            GsiEngine gsiEngine = new GsiEngine(engine, cf);
            gsiEngine.setUsingLegacyClose(isUsingLegacyClose);
            return new GsiFrameEngine(gsiEngine);
        } else {
            return engine;
        }
    }

    @Override
    public SSLEngine newSSLEngine()
    {
        return wrapEngine(super.newSSLEngine());
    }

    @Override
    public SSLEngine newSSLEngine(String host, int port)
    {
        return wrapEngine(super.newSSLEngine(host, port));
    }
}
