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

import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateFactory;
import java.util.concurrent.TimeUnit;

import org.dcache.gsi.CanlContextFactory;
import org.dcache.gsi.KeyPairCache;
import org.dcache.util.Args;
import org.dcache.util.CertificateFactories;
import org.dcache.util.Crypto;

public class GsiEngineDssContextFactory implements DssContextFactory
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

    private final CanlContextFactory factory;
    private final CertificateFactory cf;

    public GsiEngineDssContextFactory(String args) throws Exception
    {
        this(new Args(args));
    }

    public GsiEngineDssContextFactory(Args arguments) throws Exception
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

    public GsiEngineDssContextFactory(File serverKeyPath, File serverCertificatePath,
                                      File certificateAuthorityPath, String[] bannedCiphers,
                                      NamespaceCheckingMode namespaceMode, CrlCheckingMode crlMode,
                                      OCSPCheckingMode ocspMode, long keyCacheLifetime, TimeUnit keyCacheLifetimeUnit) throws Exception
    {
        cf = CertificateFactories.newX509CertificateFactory();

        factory = new CanlContextFactory();
        factory.setCertificatePath(serverCertificatePath);
        factory.setKeyPath(serverKeyPath);
        factory.setCertificateAuthorityPath(certificateAuthorityPath);
        factory.setNeedClientAuth(true);
        factory.setWantClientAuth(true);
        factory.setExcludeCipherSuites(bannedCiphers);
        factory.setGsiEnabled(true);
        factory.setKeyPairCache(new KeyPairCache(keyCacheLifetime, keyCacheLifetimeUnit));
        factory.setNamespaceMode(namespaceMode);
        factory.setCrlCheckingMode(crlMode);
        factory.setOcspCheckingMode(ocspMode);
        factory.start();
    }

    @Override
    public DssContext create(InetSocketAddress remoteSocketAddress,
                             InetSocketAddress localSocketAddress) throws IOException
    {
        return new SslEngineDssContext(factory.newSSLEngine(remoteSocketAddress), cf);
    }
}
