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

import com.google.common.base.Supplier;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyStore;

/**
 * SslContextFactory that forwards all non-updating calls to a Supplier supplied
 * SslContextFactory. All updating calls throw UnsupportedOperationException.
 * Life cycle calls are not forwarded to the delegate, so the delegate's lifecycle
 * has to be managed externally.
 *
 * The main use of this class is to be able to replace the delegate on the fly. This
 * allows the SSLContext to be recreated to eg reload key and trust stores. This
 * cannot be done without recreating the SslContextFactory since once the factory
 * is started, the SSLContext cannot be replaced.
 */
public class SupplierForwardingSslContextFactory extends SslContextFactory
{
    private Supplier<SslContextFactory> supplier;

    public SupplierForwardingSslContextFactory()
    {
        super(null);
    }

    public void setSupplier(Supplier<SslContextFactory> supplier)
    {
        this.supplier = supplier;
    }

    protected SslContextFactory getDelegate()
    {
        return supplier.get();
    }

    @Override
    public String[] getExcludeProtocols()
    {
        return getDelegate().getExcludeProtocols();
    }

    @Override
    public void setExcludeProtocols(String... protocols)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addExcludeProtocols(String... protocol)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getIncludeProtocols()
    {
        return getDelegate().getIncludeProtocols();
    }

    @Override
    public void setIncludeProtocols(String... protocols)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getExcludeCipherSuites()
    {
        return getDelegate().getExcludeCipherSuites();
    }

    @Override
    public void setExcludeCipherSuites(String... cipherSuites)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addExcludeCipherSuites(String... cipher)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getIncludeCipherSuites()
    {
        return getDelegate().getIncludeCipherSuites();
    }

    @Override
    public void setIncludeCipherSuites(String... cipherSuites)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getKeyStorePath()
    {
        return getDelegate().getKeyStorePath();
    }

    @Override
    public void setKeyStorePath(String keyStorePath)
    {
        /* The constructor of the super class calls this method, so we silently
         * ignore the call if null is passed.
         */
        if (keyStorePath != null) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public String getKeyStoreProvider()
    {
        return getDelegate().getKeyStoreProvider();
    }

    @Override
    public void setKeyStoreProvider(String keyStoreProvider)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getKeyStoreType()
    {
        return getDelegate().getKeyStoreType();
    }

    @Override
    public void setKeyStoreType(String keyStoreType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCertAlias()
    {
        return getDelegate().getCertAlias();
    }

    @Override
    public void setCertAlias(String certAlias)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyStore getTrustStore()
    {
        return getDelegate().getTrustStore();
    }

    @Override
    public void setTrustStorePath(String trustStorePath)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTrustStoreProvider()
    {
        return getDelegate().getTrustStoreProvider();
    }

    @Override
    public void setTrustStoreProvider(String trustStoreProvider)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTrustStoreType()
    {
        return getDelegate().getTrustStoreType();
    }

    @Override
    public void setTrustStoreType(String trustStoreType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getNeedClientAuth()
    {
        return getDelegate().getNeedClientAuth();
    }

    @Override
    public void setNeedClientAuth(boolean needClientAuth)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getWantClientAuth()
    {
        return getDelegate().getWantClientAuth();
    }

    @Override
    public void setWantClientAuth(boolean wantClientAuth)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValidateCerts()
    {
        return getDelegate().isValidateCerts();
    }

    @Override
    public void setValidateCerts(boolean validateCerts)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValidatePeerCerts()
    {
        return getDelegate().isValidatePeerCerts();
    }

    @Override
    public void setValidatePeerCerts(boolean validatePeerCerts)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setKeyStorePassword(String password)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setKeyManagerPassword(String password)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTrustStorePassword(String password)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getProvider()
    {
        return getDelegate().getProvider();
    }

    @Override
    public void setProvider(String provider)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getProtocol()
    {
        return getDelegate().getProtocol();
    }

    @Override
    public void setProtocol(String protocol)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSecureRandomAlgorithm()
    {
        return getDelegate().getSecureRandomAlgorithm();
    }

    @Override
    public void setSecureRandomAlgorithm(String algorithm)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSslKeyManagerFactoryAlgorithm()
    {
        return getDelegate().getSslKeyManagerFactoryAlgorithm();
    }

    @Override
    public void setSslKeyManagerFactoryAlgorithm(String algorithm)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTrustManagerFactoryAlgorithm()
    {
        return getDelegate().getTrustManagerFactoryAlgorithm();
    }

    @Override
    public boolean isTrustAll()
    {
        return getDelegate().isTrustAll();
    }

    @Override
    public void setTrustAll(boolean trustAll)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTrustManagerFactoryAlgorithm(String algorithm)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRenegotiationAllowed()
    {
        return getDelegate().isRenegotiationAllowed();
    }

    @Override
    public void setRenegotiationAllowed(boolean renegotiationAllowed)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCrlPath()
    {
        return getDelegate().getCrlPath();
    }

    @Override
    public void setCrlPath(String crlPath)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxCertPathLength()
    {
        return getDelegate().getMaxCertPathLength();
    }

    @Override
    public void setMaxCertPathLength(int maxCertPathLength)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SSLContext getSslContext()
    {
        return getDelegate().getSslContext();
    }

    @Override
    public void setSslContext(SSLContext sslContext)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setEndpointIdentificationAlgorithm(String endpointIdentificationAlgorithm)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void selectProtocols(String[] enabledProtocols, String[] supportedProtocols)
    {
        getDelegate().selectProtocols(enabledProtocols, supportedProtocols);
    }

    @Override
    public boolean isEnableCRLDP()
    {
        return getDelegate().isEnableCRLDP();
    }

    @Override
    public void setEnableCRLDP(boolean enableCRLDP)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEnableOCSP()
    {
        return getDelegate().isEnableOCSP();
    }

    @Override
    public void setEnableOCSP(boolean enableOCSP)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getOcspResponderURL()
    {
        return getDelegate().getOcspResponderURL();
    }

    @Override
    public void setOcspResponderURL(String ocspResponderURL)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setKeyStore(KeyStore keyStore)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTrustStore(KeyStore trustStore)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setKeyStoreResource(Resource resource)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTrustStoreResource(Resource resource)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSessionCachingEnabled()
    {
        return getDelegate().isSessionCachingEnabled();
    }

    @Override
    public void setSessionCachingEnabled(boolean enableSessionCaching)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSslSessionCacheSize()
    {
        return getDelegate().getSslSessionCacheSize();
    }

    @Override
    public void setSslSessionCacheSize(int sslSessionCacheSize)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSslSessionTimeout()
    {
        return getDelegate().getSslSessionTimeout();
    }

    @Override
    public void setSslSessionTimeout(int sslSessionTimeout)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SSLServerSocket newSslServerSocket(String host, int port, int backlog) throws IOException
    {
        return getDelegate().newSslServerSocket(host, port, backlog);
    }

    @Override
    public SSLSocket newSslSocket() throws IOException
    {
        return getDelegate().newSslSocket();
    }

    @Override
    public SSLEngine newSSLEngine()
    {
        return getDelegate().newSSLEngine();
    }

    @Override
    public SSLEngine newSSLEngine(String host, int port)
    {
        return getDelegate().newSSLEngine(host, port);
    }

    @Override
    public SSLEngine newSSLEngine(InetSocketAddress address)
    {
        return getDelegate().newSSLEngine(address);
    }

    @Override
    public void customize(SSLEngine sslEngine)
    {
        getDelegate().customize(sslEngine);
    }

    @Override
    public String toString()
    {
        return getDelegate().toString();
    }
}
