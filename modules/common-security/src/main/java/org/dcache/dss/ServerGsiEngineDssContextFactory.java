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
import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateFactory;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.dcache.gsi.KeyPairCache;
import org.dcache.gsi.ServerGsiEngine;
import org.dcache.ssl.CanlContextFactory;
import org.dcache.util.Args;
import org.dcache.util.CertificateFactories;
import org.dcache.util.Crypto;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.toArray;
import static java.util.Arrays.asList;

public class ServerGsiEngineDssContextFactory implements DssContextFactory
{
    private static final String SERVICE_KEY = "service_key";
    private static final String SERVICE_CERT = "service_cert";
    private static final String SERVICE_TRUSTED_CERTS = "service_trusted_certs";
    private static final String CIPHER_FLAGS = "ciphers";
    private static final String NAMESPACE_MODE = "namespace-mode";
    private static final String CRL_MODE = "crl-mode";
    private static final String OCSP_MODE = "ocsp-mode";
    private static final String KEY_CACHE_LIFETIME = "key-cache-lifetime";
    private static final String KEY_CACHE_LIFETIME_UNIT = "key-cache-lifetime-unit";

    private final CertificateFactory cf;
    private final Set<String> bannedCiphers;
    private final Set<String> bannedProtocols;
    private final Callable<SSLContext> factory;
    private final KeyPairCache keyPairCache;

    public ServerGsiEngineDssContextFactory(String args) throws Exception
    {
        this(new Args(args));
    }

    public ServerGsiEngineDssContextFactory(Args arguments) throws Exception
    {
        this(new File(arguments.getOption(SERVICE_KEY)),
             new File(arguments.getOption(SERVICE_CERT)),
             new File(arguments.getOption(SERVICE_TRUSTED_CERTS)),
             Crypto.getBannedCipherSuitesFromConfigurationValue(arguments.getOption(CIPHER_FLAGS)),
             NamespaceCheckingMode.valueOf(arguments.getOption(NAMESPACE_MODE)),
             CrlCheckingMode.valueOf(arguments.getOption(CRL_MODE)),
             OCSPCheckingMode.valueOf(arguments.getOption(OCSP_MODE)),
             arguments.getLongOption(KEY_CACHE_LIFETIME),
             TimeUnit.valueOf(arguments.getOption(KEY_CACHE_LIFETIME_UNIT)));
    }

    public ServerGsiEngineDssContextFactory(File serverKeyPath, File serverCertificatePath,
                                            File certificateAuthorityPath, String[] bannedCiphers,
                                            NamespaceCheckingMode namespaceMode, CrlCheckingMode crlMode,
                                            OCSPCheckingMode ocspMode, long keyCacheLifetime,
                                            TimeUnit keyCacheLifetimeUnit) throws Exception
    {
        cf = CertificateFactories.newX509CertificateFactory();

        this.bannedCiphers = ImmutableSet.copyOf(bannedCiphers);
        this.bannedProtocols = ImmutableSet.of("SSL", "SSLv2", "SSLv2Hello", "SSLv3");
        keyPairCache = new KeyPairCache(keyCacheLifetime, keyCacheLifetimeUnit);

        factory = CanlContextFactory.custom()
                .withCertificateAuthorityPath(certificateAuthorityPath.toPath())
                .withCrlCheckingMode(crlMode)
                .withOcspCheckingMode(ocspMode)
                .withNamespaceMode(namespaceMode)
                .withLazy(false)
                .withKeyPath(serverKeyPath.toPath())
                .withCertificatePath(serverCertificatePath.toPath())
                .buildWithCaching();
        factory.call(); // Fail fast in case of config errors
    }

    @Override
    public DssContext create(InetSocketAddress remoteSocketAddress, InetSocketAddress localSocketAddress)
            throws IOException
    {
        try {
            SSLEngine delegate = factory.call().createSSLEngine(remoteSocketAddress.getHostString(),
                                                                 remoteSocketAddress.getPort());
            SSLParameters sslParameters = delegate.getSSLParameters();
            String[] cipherSuites = toArray(filter(asList(sslParameters.getCipherSuites()), not(in(bannedCiphers))), String.class);
            String[] protocols = toArray(filter(asList(sslParameters.getProtocols()), not(in(bannedProtocols))), String.class);
            sslParameters.setCipherSuites(cipherSuites);
            sslParameters.setProtocols(protocols);
            sslParameters.setWantClientAuth(true);
            sslParameters.setNeedClientAuth(true);
            delegate.setSSLParameters(sslParameters);

            ServerGsiEngine engine = new ServerGsiEngine(delegate, cf);
            engine.setKeyPairCache(keyPairCache);
            engine.setUsingLegacyClose(true);
            return new SslEngineDssContext(engine, cf);
        } catch (Exception e) {
            Throwables.propagateIfPossible(e, IOException.class);
            throw new IOException("Failed to create SSL engine: " + e.getMessage(), e);
        }
    }
}
