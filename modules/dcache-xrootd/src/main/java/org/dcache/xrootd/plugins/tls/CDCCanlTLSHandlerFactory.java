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
package org.dcache.xrootd.plugins.tls;

import dmg.cells.nucleus.CDC;
import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import java.io.File;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import org.dcache.ssl.CanlContextFactory;

/**
 * Provides the SSL handler's context using the CanlContextFactory.
 * <p/>
 * This class's FQN should be assigned to the xrootd property:<br/>
 *
 *   xrootd.security.tls.handler-factory.class=org.dcache.xrootd.plugins.tls.CDCCanlTLSHandlerFactory
 */
public class CDCCanlTLSHandlerFactory extends SSLHandlerFactory {
  private static final String SERVICE_KEY = "xrootd.security.tls.hostcert.key";
  private static final String SERVICE_CERT = "xrootd.security.tls.hostcert.cert";
  private static final String SERVICE_CACERTS = "xrootd.security.tls.ca.path";
  private static final String NAMESPACE_MODE = "xrootd.security.tls.ca.namespace-mode";
  private static final String CRL_MODE = "xrootd.security.tls.ca.crl-mode";
  private static final String OCSP_MODE = "xrootd.security.tls.ca.ocsp-mode";

  @Override
  protected SSLContext buildContext(Properties properties) throws Exception {
    File serviceKey = new File(properties.getProperty(SERVICE_KEY));
    File serviceCert = new File(properties.getProperty(SERVICE_CERT));
    File serviceCaCerts = new File(properties.getProperty(SERVICE_CACERTS));
    NamespaceCheckingMode namespaceMode =
        NamespaceCheckingMode.valueOf(properties.getProperty(NAMESPACE_MODE));
    CrlCheckingMode crlMode
        = CrlCheckingMode.valueOf(properties.getProperty(CRL_MODE));
    OCSPCheckingMode ocspMode
        = OCSPCheckingMode.valueOf(properties.getProperty(OCSP_MODE));

    return CanlContextFactory.custom()
        .withCertificatePath(serviceCert.toPath())
        .withKeyPath(serviceKey.toPath())
        .withCertificateAuthorityPath(serviceCaCerts.toPath())
        .withCrlCheckingMode(crlMode)
        .withOcspCheckingMode(ocspMode)
        .withNamespaceMode(namespaceMode)
        .withLazy(false)
        .withLoggingContext(new CDC()::restore)
        .buildWithCaching()
        .call();
  }
}
