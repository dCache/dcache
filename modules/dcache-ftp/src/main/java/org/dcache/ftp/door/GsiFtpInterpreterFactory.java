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
package org.dcache.ftp.door;

import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;

import java.io.File;

import diskCacheV111.util.ConfigurationException;

import org.dcache.dss.GsiEngineDssContextFactory;
import org.dcache.util.Args;
import org.dcache.util.Crypto;
import org.dcache.util.Option;

public class GsiFtpInterpreterFactory extends FtpInterpreterFactory
{
    @Option(name="service-key", required=true)
    protected File service_key;

    @Option(name="service-cert", required=true)
    protected File service_cert;

    @Option(name="service-trusted-certs", required=true)
    protected File service_trusted_certs;

    @Option(name="cipher-flags", required=true)
    protected String cipherFlags;

    @Option(name="namespace-mode", required=true)
    protected NamespaceCheckingMode namespaceMode;

    @Option(name="crl-mode", required=true)
    protected CrlCheckingMode crlMode;

    @Option(name="ocsp-mode", required=true)
    protected OCSPCheckingMode ocspMode;

    private GsiEngineDssContextFactory dssContextFactory;

    @Override
    public void configure(Args args) throws ConfigurationException
    {
        super.configure(args);
        try {
            dssContextFactory = getDssContextFactory();
        } catch (Exception e) {
            throw new ConfigurationException("Failed to create security context:" + e.getMessage(), e);
        }
    }

    @Override
    protected AbstractFtpDoorV1 createInterpreter()
    {
        return new GsiFtpDoorV1(dssContextFactory);
    }

    protected GsiEngineDssContextFactory getDssContextFactory() throws Exception
    {
        return new GsiEngineDssContextFactory(service_key, service_cert, service_trusted_certs,
                                              Crypto.getBannedCipherSuitesFromConfigurationValue(cipherFlags),
                                              namespaceMode, crlMode, ocspMode);
    }
}
