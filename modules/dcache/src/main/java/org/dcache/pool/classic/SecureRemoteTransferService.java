/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.classic;

import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Required;


/**
 * This class represents any TransferService that makes a secure connection with some remote
 * service.  This is achieved by the remote service authenticating using X.509, which the pool must
 * validate as having been issued by a trustworthy certificate authority.
 */
public abstract class SecureRemoteTransferService
      extends AbstractMoverProtocolTransferService {

    private String caPath;
    private OCSPCheckingMode ocspCheckingMode;
    private CrlCheckingMode crlCheckingMode;
    private NamespaceCheckingMode namespaceMode;
    private long certificateAuthorityUpdateInterval;
    private TimeUnit certificateAuthorityUpdateIntervalUnit;

    protected final SecureRandom secureRandom = new SecureRandom();

    public String getCertificateAuthorityPath() {
        return caPath;
    }

    @Required
    public void setCertificateAuthorityPath(String certificateAuthorityPath) {
        this.caPath = certificateAuthorityPath;
    }

    public OCSPCheckingMode getOcspCheckingMode() {
        return ocspCheckingMode;
    }

    @Required
    public void setOcspCheckingMode(OCSPCheckingMode ocspCheckingMode) {
        this.ocspCheckingMode = ocspCheckingMode;
    }

    public CrlCheckingMode getCrlCheckingMode() {
        return crlCheckingMode;
    }

    @Required
    public void setCrlCheckingMode(CrlCheckingMode crlCheckingMode) {
        this.crlCheckingMode = crlCheckingMode;
    }

    public NamespaceCheckingMode getNamespaceMode() {
        return namespaceMode;
    }

    @Required
    public void setNamespaceMode(NamespaceCheckingMode namespaceMode) {
        this.namespaceMode = namespaceMode;
    }

    public long getCertificateAuthorityUpdateInterval() {
        return certificateAuthorityUpdateInterval;
    }

    @Required
    public void setCertificateAuthorityUpdateInterval(long certificateAuthorityUpdateInterval) {
        this.certificateAuthorityUpdateInterval = certificateAuthorityUpdateInterval;
    }

    public TimeUnit getCertificateAuthorityUpdateIntervalUnit() {
        return certificateAuthorityUpdateIntervalUnit;
    }

    @Required
    public void setCertificateAuthorityUpdateIntervalUnit(TimeUnit unit) {
        this.certificateAuthorityUpdateIntervalUnit = unit;
    }
}
