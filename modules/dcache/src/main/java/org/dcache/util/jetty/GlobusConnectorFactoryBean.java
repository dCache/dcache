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
package org.dcache.util.jetty;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import org.dcache.gsi.GlobusContextFactory;
import org.dcache.gsi.SupplierForwardingSslContextFactory;

import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.dcache.util.Crypto.*;

public class GlobusConnectorFactoryBean implements FactoryBean<ServerConnector>
{
    private final Supplier<SslContextFactory> contextFactorySupplier =
            new Supplier<SslContextFactory>()
            {
                @Override
                public SslContextFactory get()
                {
                    try {
                        return createContextFactory();
                    } catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                }
            };

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
    private String serverCertificatePath;
    private String serverKeyPath;
    private String certificateAuthorityPath;
    private boolean needClientAuth;
    private boolean wantClientAuth;
    private String[] excludedCipherSuites = {};
    private boolean enableGsi;
    private boolean isUsingLegacyClose;

    public int getAcceptors()
    {
        return acceptors;
    }

    public void setAcceptors(int acceptors)
    {
        this.acceptors = acceptors;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getBacklog()
    {
        return backlog;
    }

    public void setBacklog(int backlog)
    {
        this.backlog = backlog;
    }

    public long getIdleTimeout()
    {
        return idleTimeout;
    }

    @Required
    public void setIdleTimeout(long idleTimeout)
    {
        this.idleTimeout = idleTimeout;
    }

    public TimeUnit getIdleTimeoutUnit()
    {
        return idleTimeoutUnit;
    }

    @Required
    public void setIdleTimeoutUnit(TimeUnit idleTimeoutUnit)
    {
        this.idleTimeoutUnit = idleTimeoutUnit;
    }

    public Server getServer()
    {
        return server;
    }

    @Required
    public void setServer(Server server)
    {
        this.server = server;
    }

    public long getServerCertificateTimeout()
    {
        return hostCertificateTimeout;
    }

    @Required
    public void setServerCertificateTimeout(long serverCertificateTimeout)
    {
        this.hostCertificateTimeout = serverCertificateTimeout;
    }

    public TimeUnit getServerCertificateTimeoutUnit()
    {
        return hostCertificateTimeoutUnit;
    }

    @Required
    public void setServerCertificateTimeoutUnit(TimeUnit serverCertificateTimeoutUnit)
    {
        this.hostCertificateTimeoutUnit = serverCertificateTimeoutUnit;
    }

    public long getCaPathTimeout()
    {
        return caCertificateTimeout;
    }

    @Required
    public void setCaPathTimeout(long caPathTimeout)
    {
        this.caCertificateTimeout = caPathTimeout;
    }

    public TimeUnit getCaPathTimeoutUnit()
    {
        return caCertificateTimeoutUnit;
    }

    @Required
    public void setCaPathTimeoutUnit(TimeUnit caPathTimeoutUnit)
    {
        this.caCertificateTimeoutUnit = caPathTimeoutUnit;
    }

    public String getServerCertificatePath()
    {
        return serverCertificatePath;
    }

    @Required
    public void setServerCertificatePath(String serverCertificatePath)
    {
        this.serverCertificatePath = serverCertificatePath;
    }

    public String getServerKeyPath()
    {
        return serverKeyPath;
    }

    @Required
    public void setServerKeyPath(String serverKeyPath)
    {
        this.serverKeyPath = serverKeyPath;
    }

    public String getCaPath()
    {
        return certificateAuthorityPath;
    }

    @Required
    public void setCaPath(String caPath)
    {
        this.certificateAuthorityPath = caPath;
    }

    public boolean isNeedClientAuth()
    {
        return needClientAuth;
    }

    public void setNeedClientAuth(boolean needClientAuth)
    {
        this.needClientAuth = needClientAuth;
    }

    public boolean isWantClientAuth()
    {
        return wantClientAuth;
    }

    public void setWantClientAuth(boolean wantClientAuth)
    {
        this.wantClientAuth = wantClientAuth;
    }

    public String[] getExcludedCipherSuites()
    {
        return excludedCipherSuites;
    }

    public void setExcludedCipherSuites(String[] excludedCipherSuites)
    {
        this.excludedCipherSuites = excludedCipherSuites;
    }

    public void setCipherFlags(String cipherFlags)
    {
        this.excludedCipherSuites = getBannedCipherSuitesFromConfigurationValue(cipherFlags);
    }

    public boolean isEnableGsi()
    {
        return enableGsi;
    }

    public void setEnableGsi(boolean enableGsi)
    {
        this.enableGsi = enableGsi;
    }

    public boolean isUsingLegacyClose()
    {
        return isUsingLegacyClose;
    }

    public void setUsingLegacyClose(boolean isUsingLegacyClose)
    {
        this.isUsingLegacyClose = isUsingLegacyClose;
    }

    private SslContextFactory createContextFactory() throws Exception
    {
        GlobusContextFactory factory = new GlobusContextFactory();
        factory.setServerCertificatePath(serverCertificatePath);
        factory.setServerKeyPath(serverKeyPath);
        factory.setTrustStorePath(certificateAuthorityPath);
        factory.setNeedClientAuth(needClientAuth);
        factory.setWantClientAuth(wantClientAuth);
        factory.setExcludeCipherSuites(excludedCipherSuites);
        factory.setEnableGsi(enableGsi);
        factory.setUsingLegacyClose(isUsingLegacyClose);
        factory.start();
        return factory;
    }

    @Override
    public ServerConnector getObject() throws Exception
    {
        long timeout = Math.min(hostCertificateTimeoutUnit.toMillis(hostCertificateTimeout),
                                caCertificateTimeoutUnit.toMillis(caCertificateTimeout));
        Supplier<SslContextFactory> supplier =
                Suppliers.memoizeWithExpiration(contextFactorySupplier, timeout, MILLISECONDS);
        supplier.get(); // Trigger early instantiation to uncover configuration errors

        SupplierForwardingSslContextFactory sslContextFactory = new SupplierForwardingSslContextFactory();
        sslContextFactory.setSupplier(supplier);

        HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory();
        httpConnectionFactory.getHttpConfiguration().addCustomizer(new SecureRequestCustomizer());
        if (enableGsi) {
            httpConnectionFactory.getHttpConfiguration().addCustomizer(new GsiRequestCustomizer());
        }

        SslConnectionFactory sslConnectionFactory =
                new SslConnectionFactory(sslContextFactory,
                                         httpConnectionFactory.getProtocol());

        ServerConnector serverConnector =
                new ServerConnector(server, null, null, null, acceptors, -1,
                                    sslConnectionFactory, httpConnectionFactory);
        serverConnector.setPort(port);
        serverConnector.setHost(host);
        serverConnector.setAcceptQueueSize(backlog);
        serverConnector.setIdleTimeout(idleTimeoutUnit.toMillis(idleTimeout));
        return serverConnector;
    }

    @Override
    public Class<?> getObjectType()
    {
        return ServerConnector.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }
}