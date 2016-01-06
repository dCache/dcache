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
package org.dcache.dss;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import eu.emi.security.authn.x509.X509Credential;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateFactory;
import java.util.Set;

import org.dcache.gsi.ClientGsiEngine;
import org.dcache.ssl.SslContextFactory;
import org.dcache.util.CertificateFactories;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.toArray;
import static java.util.Arrays.asList;

public class ClientGsiEngineDssContextFactory implements DssContextFactory
{
    private final CertificateFactory cf;
    private final Set<String> bannedCiphers;
    private final Set<String> bannedProtocols;
    private final SslContextFactory contextFactory;
    private boolean isDelegationEnabled;
    private boolean isDelegationLimited;
    private X509Credential credential;

    public ClientGsiEngineDssContextFactory(SslContextFactory contextFactory, X509Credential credential,
                                            String[] bannedCiphers,
                                            boolean isDelegationEnabled, boolean isDelegationLimited)
    {
        this.cf = CertificateFactories.newX509CertificateFactory();
        this.credential = credential;
        this.contextFactory = contextFactory;
        this.isDelegationEnabled = isDelegationEnabled;
        this.isDelegationLimited = isDelegationLimited;
        this.bannedCiphers = ImmutableSet.copyOf(bannedCiphers);
        this.bannedProtocols = ImmutableSet.of("SSL", "SSLv2", "SSLv2Hello", "SSLv3");
    }

    @Override
    public DssContext create(InetSocketAddress remoteSocketAddress, InetSocketAddress localSocketAddress)
            throws IOException
    {
        try {
            SSLEngine delegate =
                    contextFactory.getContext(credential).createSSLEngine(
                            remoteSocketAddress.getHostString(),
                            remoteSocketAddress.getPort());
            SSLParameters sslParameters = delegate.getSSLParameters();
            String[] cipherSuites = toArray(filter(asList(sslParameters.getCipherSuites()), not(in(bannedCiphers))), String.class);
            String[] protocols = toArray(filter(asList(sslParameters.getProtocols()), not(in(bannedProtocols))), String.class);
            sslParameters.setCipherSuites(cipherSuites);
            sslParameters.setProtocols(protocols);
            sslParameters.setWantClientAuth(true);
            sslParameters.setNeedClientAuth(true);
            delegate.setSSLParameters(sslParameters);

            ClientGsiEngine engine = new ClientGsiEngine(delegate, credential, isDelegationEnabled, isDelegationLimited);
            return new SslEngineDssContext(engine, cf);
        } catch (Exception e) {
            Throwables.propagateIfPossible(e, IOException.class);
            throw new IOException("Failed to create SSL engine: " + e.getMessage(), e);
        }
    }
}
