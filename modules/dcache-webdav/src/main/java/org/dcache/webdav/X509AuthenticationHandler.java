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
package org.dcache.webdav;

import io.milton.http.AuthenticationHandler;
import io.milton.http.Request;
import io.milton.resource.Resource;
import io.milton.servlet.ServletRequest;
import org.globus.gsi.bc.BouncyCastleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

import diskCacheV111.srm.dcache.DCacheAuthorization;

/**
 * Supports authentication from X509 client certificates.
 *
 * No actual authentication is performed as that happens during the SSL handshake. The presence
 * of a client certificate is however detected and the DN of the client certificate is passed
 * on as a user name.
 *
 * Some logic in Milton relies on knowing whether a request is anonymous or authenticated and this
 * handler allows sessions that were authenticated using an SSL client certificate to be
 * treated as authenticated by Milton.
 */
public class X509AuthenticationHandler implements AuthenticationHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DCacheAuthorization.class);

    @Override
    public boolean supports(Resource r, Request request)
    {
        Object chain = ServletRequest.getRequest().getAttribute(SecurityFilter.X509_CERTIFICATE_ATTRIBUTE);
        return (chain != null);
    }

    @Override
    public Object authenticate(Resource resource, Request request)
    {
        try {
            X509Certificate[] chain =
                    (X509Certificate[]) ServletRequest.getRequest().getAttribute(
                            SecurityFilter.X509_CERTIFICATE_ATTRIBUTE);
            String dn = BouncyCastleUtil.getIdentity(BouncyCastleUtil.getIdentityCertificate(chain));
            return resource.authenticate(dn, null);
        } catch (CertificateException e) {
            logger.warn("Failed to extract DN from certificate chain: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public void appendChallenges(Resource resource, Request request, List<String> challenges)
    {
        // don't issue challenges
    }

    @Override
    public boolean isCompatible(Resource resource, Request request)
    {
        // never issue challenges
        return false;
    }

    @Override
    public boolean credentialsPresent(Request request)
    {
        Object chain = ServletRequest.getRequest().getAttribute(SecurityFilter.X509_CERTIFICATE_ATTRIBUTE);
        return chain != null;
    }
}
