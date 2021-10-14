/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.security.trust;

import static java.util.Objects.requireNonNull;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import javax.net.ssl.X509TrustManager;

/**
 * Aggregate multiple X509TrustManager instances where a certificate chain is accepted if at least
 * one of the X509TrustManager instances accepts it.
 */
public class AggregateX509TrustManager implements X509TrustManager {

    private final List<X509TrustManager> trustManagers;

    public AggregateX509TrustManager(List<X509TrustManager> managers) {
        trustManagers = requireNonNull(managers);
    }

    @FunctionalInterface
    private interface CertificateCheck {

        void appliedTo(X509TrustManager manager) throws CertificateException;
    }

    private void genericCheck(CertificateCheck check) throws CertificateException {
        if (trustManagers.isEmpty()) {
            throw new CertificateException("No certificates are trusted.");
        }

        StringBuilder errorMessage = null;

        for (var tm : trustManagers) {
            try {
                check.appliedTo(tm);
                return;
            } catch (CertificateException e) {
                if (e.getMessage() != null) {
                    if (errorMessage == null) {
                        errorMessage = new StringBuilder();
                    } else {
                        errorMessage.append("; ");

                    }
                    errorMessage.append(e.getMessage());
                }
            }
        }

        throw errorMessage == null
              ? new CertificateException()
              : new CertificateException(errorMessage.toString());
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
          throws CertificateException {
        genericCheck(tm -> tm.checkClientTrusted(chain, authType));
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
          throws CertificateException {
        genericCheck(tm -> tm.checkServerTrusted(chain, authType));
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return trustManagers.stream()
              .map(X509TrustManager::getAcceptedIssuers)
              .flatMap(Arrays::stream)
              .toArray(X509Certificate[]::new);
    }
}
