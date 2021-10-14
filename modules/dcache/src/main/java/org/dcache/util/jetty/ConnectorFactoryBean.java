/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014-2015 Deutsches Elektronen-Synchrotron
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

import static com.google.common.base.Preconditions.checkState;
import static org.dcache.util.Crypto.getBannedCipherSuitesFromConfigurationValue;
import static org.dcache.util.jetty.ConnectorFactoryBean.Protocol.PLAIN;

import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.dcache.gsi.KeyPairCache;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ProxyConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

public class ConnectorFactoryBean implements FactoryBean<ServerConnector> {

    private int acceptors = -1;
    private int port;
    private String host;
    private int backlog;
    private long idleTimeout;
    private TimeUnit idleTimeoutUnit;
    private Server server;
    private long hostCertificateTimeout;
    private TimeUnit hostCertificateTimeoutUnit;
    private long caCertificateTimeout;
    private TimeUnit caCertificateTimeoutUnit;
    private File serverCertificatePath;
    private File serverKeyPath;
    private File certificateAuthorityPath;
    private boolean needClientAuth;
    private boolean wantClientAuth;
    private String[] excludedCipherSuites = {};
    private boolean isUsingLegacyClose;
    private CrlCheckingMode crlCheckingMode = CrlCheckingMode.IF_VALID;
    private OCSPCheckingMode ocspCheckingMode = OCSPCheckingMode.IF_AVAILABLE;
    private NamespaceCheckingMode namespaceMode = NamespaceCheckingMode.EUGRIDPMA_GLOBUS;
    private KeyPairCache keyPairCache;

    private boolean isProxyConnectionEnabled;
    private boolean isForwardedHeaderProcessingEnabled;

    private Protocol protocol;

    public int getAcceptors() {
        return acceptors;
    }

    public void setAcceptors(int acceptors) {
        this.acceptors = acceptors;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    @Required
    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public TimeUnit getIdleTimeoutUnit() {
        return idleTimeoutUnit;
    }

    @Required
    public void setIdleTimeoutUnit(TimeUnit idleTimeoutUnit) {
        this.idleTimeoutUnit = idleTimeoutUnit;
    }

    public Server getServer() {
        return server;
    }

    @Required
    public void setServer(Server server) {
        this.server = server;
    }

    public long getServerCertificateTimeout() {
        return hostCertificateTimeout;
    }

    public void setServerCertificateTimeout(long serverCertificateTimeout) {
        this.hostCertificateTimeout = serverCertificateTimeout;
    }

    public TimeUnit getServerCertificateTimeoutUnit() {
        return hostCertificateTimeoutUnit;
    }

    public void setServerCertificateTimeoutUnit(TimeUnit serverCertificateTimeoutUnit) {
        this.hostCertificateTimeoutUnit = serverCertificateTimeoutUnit;
    }

    public long getCaPathTimeout() {
        return caCertificateTimeout;
    }

    public void setCaPathTimeout(long caPathTimeout) {
        this.caCertificateTimeout = caPathTimeout;
    }

    public TimeUnit getCaPathTimeoutUnit() {
        return caCertificateTimeoutUnit;
    }

    public void setCaPathTimeoutUnit(TimeUnit caPathTimeoutUnit) {
        this.caCertificateTimeoutUnit = caPathTimeoutUnit;
    }

    public File getServerCertificatePath() {
        return serverCertificatePath;
    }

    public void setServerCertificatePath(File serverCertificatePath) {
        this.serverCertificatePath = serverCertificatePath;
    }

    public File getServerKeyPath() {
        return serverKeyPath;
    }

    public void setServerKeyPath(File serverKeyPath) {
        this.serverKeyPath = serverKeyPath;
    }

    public File getCaPath() {
        return certificateAuthorityPath;
    }

    public void setCaPath(File caPath) {
        this.certificateAuthorityPath = caPath;
    }

    public boolean isNeedClientAuth() {
        return needClientAuth;
    }

    public void setNeedClientAuth(boolean needClientAuth) {
        this.needClientAuth = needClientAuth;
    }

    public boolean isWantClientAuth() {
        return wantClientAuth;
    }

    public void setWantClientAuth(boolean wantClientAuth) {
        this.wantClientAuth = wantClientAuth;
    }

    public String[] getExcludedCipherSuites() {
        return excludedCipherSuites;
    }

    public void setExcludedCipherSuites(String[] excludedCipherSuites) {
        this.excludedCipherSuites = excludedCipherSuites;
    }

    public void setCipherFlags(String cipherFlags) {
        this.excludedCipherSuites = getBannedCipherSuitesFromConfigurationValue(cipherFlags);
    }

    @Required
    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public boolean isUsingLegacyClose() {
        return isUsingLegacyClose;
    }

    public void setUsingLegacyClose(boolean isUsingLegacyClose) {
        this.isUsingLegacyClose = isUsingLegacyClose;
    }

    public CrlCheckingMode getCrlCheckingMode() {
        return crlCheckingMode;
    }

    public void setCrlCheckingMode(CrlCheckingMode crlCheckingMode) {
        this.crlCheckingMode = crlCheckingMode;
    }

    public OCSPCheckingMode getOcspCheckingMode() {
        return ocspCheckingMode;
    }

    public void setOcspCheckingMode(OCSPCheckingMode ocspCheckingMode) {
        this.ocspCheckingMode = ocspCheckingMode;
    }

    public NamespaceCheckingMode getNamespaceMode() {
        return namespaceMode;
    }

    public void setNamespaceMode(NamespaceCheckingMode namespaceMode) {
        this.namespaceMode = namespaceMode;
    }

    public KeyPairCache getKeyPairCache() {
        return keyPairCache;
    }

    public void setKeyPairCache(KeyPairCache keyPairCache) {
        this.keyPairCache = keyPairCache;
    }

    public boolean isProxyConnectionEnabled() {
        return isProxyConnectionEnabled;
    }

    public void setProxyConnectionEnabled(boolean proxyConnectionEnabled) {
        isProxyConnectionEnabled = proxyConnectionEnabled;
    }

    public boolean isForwardedHeaderProcessingEnabled() {
        return isForwardedHeaderProcessingEnabled;
    }

    public void setForwardedHeaderProcessingEnabled(boolean forwardedHeaderProcessingEnabled) {
        isForwardedHeaderProcessingEnabled = forwardedHeaderProcessingEnabled;
    }

    private SslContextFactory createContextFactory() throws Exception {
        CanlContextFactory factory = new CanlContextFactory();
        factory.setCertificatePath(serverCertificatePath);
        factory.setKeyPath(serverKeyPath);
        factory.setCertificateAuthorityPath(certificateAuthorityPath);
        factory.setNeedClientAuth(needClientAuth);
        factory.setWantClientAuth(wantClientAuth);
        factory.setExcludeCipherSuites(excludedCipherSuites);
        factory.setGsiEnabled(protocol == Protocol.GSI);
        factory.setUsingLegacyClose(isUsingLegacyClose);
        factory.setKeyPairCache(keyPairCache);
        factory.setCertificateAuthorityUpdateInterval(
              caCertificateTimeoutUnit.toMillis(caCertificateTimeout));
        factory.setCredentialUpdateInterval(
              hostCertificateTimeoutUnit.toMillis(hostCertificateTimeout));
        factory.setNamespaceMode(namespaceMode);
        factory.setCrlCheckingMode(crlCheckingMode);
        factory.setOcspCheckingMode(ocspCheckingMode);
        factory.start();
        return factory;
    }

    @Override
    public ServerConnector getObject() throws Exception {
        checkState(protocol == PLAIN || hostCertificateTimeout > 0);
        checkState(protocol == PLAIN || hostCertificateTimeoutUnit != null);
        checkState(protocol == PLAIN || caCertificateTimeout > 0);
        checkState(protocol == PLAIN || caCertificateTimeoutUnit != null);
        checkState(protocol == PLAIN || serverCertificatePath != null);
        checkState(protocol == PLAIN || serverKeyPath != null);
        checkState(protocol == PLAIN || certificateAuthorityPath != null);

        HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory();

        switch (protocol) {
            case PLAIN:
                break;
            case TLS:
                httpConnectionFactory.getHttpConfiguration()
                      .addCustomizer(new SecureRequestCustomizer());
                break;
            case GSI:
                httpConnectionFactory.getHttpConfiguration()
                      .addCustomizer(new SecureRequestCustomizer());
                httpConnectionFactory.getHttpConfiguration()
                      .addCustomizer(new GsiRequestCustomizer());
                break;
        }

        if (isForwardedHeaderProcessingEnabled) {
            httpConnectionFactory.getHttpConfiguration()
                  .addCustomizer(new ForwardedRequestCustomizer());
        }

        List<ConnectionFactory> factories = new ArrayList<>();
        if (isProxyConnectionEnabled) {
            factories.add(new ProxyConnectionFactory());
        }
        if (protocol != PLAIN) {
            factories.add(new SslConnectionFactory(createContextFactory(),
                  httpConnectionFactory.getProtocol()));
        }
        factories.add(httpConnectionFactory);

        ServerConnector serverConnector =
              new ServerConnector(server, null, null, null, acceptors, -1,
                    factories.toArray(ConnectionFactory[]::new));
        serverConnector.setPort(port);
        serverConnector.setHost(host);
        serverConnector.setAcceptQueueSize(backlog);
        serverConnector.setIdleTimeout(idleTimeoutUnit.toMillis(idleTimeout));
        return serverConnector;
    }

    @Override
    public Class<?> getObjectType() {
        return ServerConnector.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public enum Protocol {
        PLAIN, TLS, GSI
    }
}