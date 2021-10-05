/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.xrootd.security;

import static com.google.common.collect.Iterables.getFirst;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_IOError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotAuthorized;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ServerError;

import eu.emi.security.authn.x509.X509Credential;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Map;
import javax.security.auth.Subject;
import org.dcache.auth.FQANPrincipal;
import org.dcache.gsi.KeyPairCache;
import org.dcache.gsi.X509Delegation;
import org.dcache.gsi.X509DelegationHelper;
import org.dcache.util.CertificateFactories;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.plugins.authn.gsi.SerializableX509Credential;
import org.dcache.xrootd.plugins.authn.gsi.X509ProxyDelegationClient;
import org.dcache.xrootd.util.ProxyRequest;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.italiangrid.voms.ac.VOMSValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements xrootd4j proxy delegation client API.
 */
public class GSIProxyDelegationClient extends X509ProxyDelegationClient {

    private static final Logger LOGGER
          = LoggerFactory.getLogger(GSIProxyDelegationClient.class);

    private static final CertificateFactory CERTIFICATE_FACTORY
          = CertificateFactories.newX509CertificateFactory();

    private final VOMSACValidator vomsValidator;
    private final Map<String, X509Delegation> delegations;
    private final KeyPairCache keyPairCache;

    GSIProxyDelegationClient(ProxyDelegationStore store) {
        requireNonNull(store, "GSIProxyDelegationClient "
              + "cannot be constructed with null store.");
        vomsValidator = store.vomsValidator;
        delegations = store.delegations;
        keyPairCache = store.keyPairCache;
    }

    @Override
    public void cancelProxyRequest(ProxyRequest proxyRequest) {
        String id = proxyRequest.getId();
        LOGGER.debug("cancelProxyRequest: removing delegation object for {}.", id);
        delegations.remove(id);
    }

    @Override
    public void close() {
        // NOP
    }

    @Override
    public SerializableX509Credential finalizeProxyCredential(String id,
          String proxy)
          throws XrootdException {
        LOGGER.debug("finalizeProxyCredential: "
                    + "removing delegation object for {}.",
              id);
        X509Delegation delegation = delegations.remove(id);

        if (delegation == null) {
            throw new XrootdException(kXR_ServerError,
                  "internal error during finalize proxy;"
                        + "cannot find delegation for: "
                        + id);
        }

        try {
            X509Credential credential
                  = X509DelegationHelper.acceptCertificate(proxy,
                  delegation);
            return new SerializableX509Credential(credential.getCertificateChain(),
                  credential.getKey());
        } catch (GeneralSecurityException e) {
            throw new XrootdException(kXR_NotAuthorized,
                  "error during finalize proxy;"
                        + "accept certificate failed for: "
                        + id);
        }
    }

    @Override
    public ProxyRequest getProxyRequest(X509Certificate[] certChain)
          throws XrootdException {
        Subject subject = createSubject(certChain);
        LOGGER.debug("creating new Proxy Request (CSR) for {}.",
              subject.getPrincipals());

        try {
            CertPath path = getFirst(subject.getPublicCredentials(CertPath.class),
                  null);
            X509Delegation delegation
                  = X509DelegationHelper.newDelegation(path, keyPairCache);

            delegation.setPemRequest(
                  X509DelegationHelper.createRequest(delegation.getCertificates(),
                        delegation.getKeyPair()));

            LOGGER.debug("registering delegation object {} for {}.",
                  delegation, delegation.getId());
            delegations.put(delegation.getId(), delegation);

            return new ProxyRequest(certChain,
                  delegation.getId(),
                  delegation.getPemRequest());
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError,
                  "could not create new Proxy Request (CSR) for "
                        + subject + ": "
                        + e.getMessage());
        } catch (GeneralSecurityException e) {
            throw new XrootdException(kXR_NotAuthorized,
                  "could not create new Proxy Request (CSR) for "
                        + subject + ": "
                        + e.getMessage());
        }
    }

    private Subject createSubject(X509Certificate[] certChain)
          throws XrootdException {
        Subject subject = new Subject();

        X509Certificate cert = certChain[certChain.length - 1];

        GlobusPrincipal principal = new GlobusPrincipal(cert.getSubjectDN()
              .toString());

        LOGGER.debug("createSubject for {}, principal {}.",
              cert.getSubjectDN(), principal);

        subject.getPrincipals().add(principal);

        vomsValidator.validateWithResult(certChain)
              .stream()
              .filter(VOMSValidationResult::isValid)
              .map(VOMSValidationResult::getAttributes)
              .findFirst()
              .ifPresent(a -> subject.getPrincipals()
                    .add(new FQANPrincipal(a.getFQANs()
                          .stream()
                          .findFirst()
                          .get(),
                          true)));
        try {
            subject.getPublicCredentials()
                  .add(CERTIFICATE_FACTORY.generateCertPath(asList(certChain)));
            return subject;
        } catch (CertificateException e) {
            throw new XrootdException(kXR_ServerError,
                  "error creating public credentials for "
                        + subject);
        }
    }
}
