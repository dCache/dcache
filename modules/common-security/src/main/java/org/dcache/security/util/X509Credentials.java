/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.security.util;

import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.proxy.ProxyChainInfo;
import org.bouncycastle.asn1.x509.AttributeCertificate;
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.VOMSError;
import org.italiangrid.voms.asn1.VOMSACUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 *  Utility methods for X509Credential objects.
 */
public class X509Credentials
{
    private static final Logger LOGGER = LoggerFactory.getLogger(X509Credentials.class);

    private static final AttributeCertificate[] EMPTY_ARRAY = {};

    private X509Credentials()
    {
        // Prevent instantiation
    }

    /**
     * Calculate when this credential will expire.  If the credential contains
     * AttributeCertificates then this is taken into account.
     * @param credential the credential to examine
     * @return when this credential will expire
     */
    public static Optional<Instant> calculateExpiry(X509Credential credential)
    {
        Optional<Instant> expiry = Stream.of(credential.getCertificateChain())
                        .map(X509Certificate::getNotAfter)
                        .min(Date::compareTo)
                        .map(Date::toInstant);

        Map<String,Instant> expiryPerVo = new HashMap<>();
        try {
            ProxyChainInfo info = new ProxyChainInfo(credential.getCertificateChain());
            for (AttributeCertificate[] acForCertificateOrNull : info.getAttributeCertificateExtensions()) {
                AttributeCertificate[] acForCertificate = acForCertificateOrNull == null ? EMPTY_ARRAY : acForCertificateOrNull;
                for (AttributeCertificate ac : acForCertificate) {
                    try {
                        VOMSAttribute attribute = VOMSACUtils.deserializeVOMSAttributes(ac);
                        Instant acExpires = attribute.getNotAfter().toInstant();

                        /* It is allowed for an X.509 chain to have multiple
                         * ACs from the same VOMS server.  For example, if the
                         * X.509 chain is valid for one week, but the VOMS
                         * server issues ACs that are valid for one day then
                         * the AC may be renewed by contacting the VOMS server
                         * shortly before the AC certificate expires, requesting
                         * a new AC and creating a new proxy credential with
                         * this new AC.
                         *
                         * Therefore, we consider all ACs for the same VO as
                         * equivalent and take the AC expiry that is furthest in
                         * the future.
                         */
                        String vo = attribute.getVO();
                        expiryPerVo.merge(vo, acExpires, (a,b) -> a.isAfter(b) ? a : b);
                    } catch (VOMSError e) {
                        LOGGER.warn("Badly formatted VOMS AC: {}", e.toString());
                    }
                }
            }
        } catch (IOException | CertificateException e) {
            LOGGER.warn("Unable to parse AC: {}", e.toString());
        }

        // Since we cannot know which VO is important if the credential
        // contains multiple, consider the ACs expired if any VO has expired.
        Optional<Instant> acExpiry = expiryPerVo.values().stream().sorted().findFirst();

        if (!expiry.isPresent() || (acExpiry.isPresent() && acExpiry.get().isBefore(expiry.get()))) {
            expiry = acExpiry;
        }

        return expiry;
    }
}
