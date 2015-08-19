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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateFactory;

import org.dcache.gsi.GlobusContextFactory;
import org.dcache.util.Args;
import org.dcache.util.CertificateFactories;
import org.dcache.util.Crypto;

public class GsiEngineDssContextFactory implements DssContextFactory
{
    private static final String SERVICE_KEY = "service_key";
    private static final String SERVICE_CERT = "service_cert";
    private static final String SERVICE_TRUSTED_CERTS = "service_trusted_certs";
    private static final String CIPHER_FLAGS = "ciphers";

    private final GlobusContextFactory factory;
    private final CertificateFactory cf;

    public GsiEngineDssContextFactory(String args) throws Exception
    {
        this(new Args(args));
    }

    public GsiEngineDssContextFactory(Args arguments) throws Exception
    {
        this(arguments.getOption(SERVICE_KEY),
             arguments.getOption(SERVICE_CERT),
             arguments.getOption(SERVICE_TRUSTED_CERTS),
             Crypto.getBannedCipherSuitesFromConfigurationValue(arguments.getOption(CIPHER_FLAGS)));
    }

    public GsiEngineDssContextFactory(String serverKeyPath, String serverCertificatePath,
                                      String certificateAuthorityPath, String[] bannedCiphers) throws Exception
    {
        cf = CertificateFactories.newX509CertificateFactory();

        factory = new GlobusContextFactory();
        factory.setServerCertificatePath(serverCertificatePath);
        factory.setServerKeyPath(serverKeyPath);
        factory.setTrustStorePath(certificateAuthorityPath);
        factory.setNeedClientAuth(true);
        factory.setWantClientAuth(true);
        factory.setExcludeCipherSuites(bannedCiphers);
        factory.setEnableGsi(true);
        factory.start();
    }

    @Override
    public DssContext create(InetSocketAddress remoteSocketAddress,
                             InetSocketAddress localSocketAddress) throws IOException
    {
        return new SslEngineDssContext(factory.newSSLEngine(remoteSocketAddress), cf);
    }
}
