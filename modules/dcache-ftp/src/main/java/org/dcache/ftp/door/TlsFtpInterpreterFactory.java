/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.ConfigurationException;
import diskCacheV111.util.FsPath;

import dmg.cells.nucleus.CDC;

import org.dcache.ssl.CanlContextFactory;
import org.dcache.util.Args;
import org.dcache.util.Option;

public class TlsFtpInterpreterFactory extends FtpInterpreterFactory
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

    @Option(name="key-cache-lifetime", required=true)
    private long keyCacheLifetime;

    @Option(name="key-cache-lifetime-unit", required=true)
    private TimeUnit keyCacheLifetimeUnit;

    @Option(name="username-password-enabled", required=true)
    private boolean allowUsernamePassword;

    @Option(name="anonymous-enabled", required=true)
    private boolean anonymousEnabled;

    @Option(name="anonymous-user", required=true)
    private String anonymousUser;

    @Option(name="anonymous-email-required", required=true)
    private boolean requireAnonEmailPassword;

    @Option(name="anonymous-root", required=true)
    private FsPath anonymousRoot;

    private Optional<String> anonUser;

    private SSLContext sslContext;

    @Override
    public void configure(Args args) throws ConfigurationException
    {
        super.configure(args);
        try {
            sslContext = buildContext();
        } catch (Exception e) {
            throw new ConfigurationException("Unable to create SSLContext: " + e.getMessage());
        }

        anonUser = anonymousEnabled
                ? Optional.of(anonymousUser)
                : Optional.empty();
    }

    @Override
    protected AbstractFtpDoorV1 createInterpreter()
    {
        SSLEngine engine = sslContext.createSSLEngine();
        engine.setNeedClientAuth(false);

        /* REVISIT: with FTPS, it is possible for a client to send an X.509
         * credential as part of the TLS handshake.  Seemingly most FTPS clients
         * do not support this option (but curl is an example of a client that
         * does).  Therefore, the code currently does not support X.509 based
         * authentication.
         */
        engine.setWantClientAuth(false);

        return new TlsFtpDoor(engine, allowUsernamePassword, anonUser,
                anonymousRoot, requireAnonEmailPassword);
    }

    protected SSLContext buildContext() throws Exception
    {
        return CanlContextFactory.custom()
                .withCertificatePath(service_cert.toPath())
                .withKeyPath(service_key.toPath())
                .withCertificateAuthorityPath(service_trusted_certs.toPath())
                .withCrlCheckingMode(crlMode)
                .withOcspCheckingMode(ocspMode)
                .withNamespaceMode(namespaceMode)
                .withLazy(false)
                .withLoggingContext(new CDC()::restore)
                .buildWithCaching()
                .call();
    }

}
