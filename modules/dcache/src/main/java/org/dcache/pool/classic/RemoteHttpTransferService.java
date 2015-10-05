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
package org.dcache.pool.classic;

import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import eu.emi.security.authn.x509.OCSPParametes;
import eu.emi.security.authn.x509.ProxySupport;
import eu.emi.security.authn.x509.RevocationParameters;
import eu.emi.security.authn.x509.impl.OpensslCertChainValidator;
import eu.emi.security.authn.x509.impl.ValidatorParams;
import org.springframework.beans.factory.annotation.Required;

import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpsDataTransferProtocolInfo;

import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.movers.RemoteHttpDataTransferProtocol_1;
import org.dcache.pool.movers.RemoteHttpsDataTransferProtocol_1;

public class RemoteHttpTransferService extends AbstractMoverProtocolTransferService
{
    private String caPath;
    private OCSPCheckingMode ocspCheckingMode;
    private CrlCheckingMode crlCheckingMode;
    private NamespaceCheckingMode namespaceMode;
    private long certificateAuthorityUpdateInterval;

    private OpensslCertChainValidator validator;
    private final SecureRandom secureRandom = new SecureRandom();
    private TimeUnit certificateAuthorityUpdateIntervalUnit;

    public String getCertificateAuthorityPath()
    {
        return caPath;
    }

    @Required
    public void setCertificateAuthorityPath(String certificateAuthorityPath)
    {
        this.caPath = certificateAuthorityPath;
    }

    public OCSPCheckingMode getOcspCheckingMode()
    {
        return ocspCheckingMode;
    }

    @Required
    public void setOcspCheckingMode(OCSPCheckingMode ocspCheckingMode)
    {
        this.ocspCheckingMode = ocspCheckingMode;
    }

    public CrlCheckingMode getCrlCheckingMode()
    {
        return crlCheckingMode;
    }

    @Required
    public void setCrlCheckingMode(CrlCheckingMode crlCheckingMode)
    {
        this.crlCheckingMode = crlCheckingMode;
    }

    public NamespaceCheckingMode getNamespaceMode()
    {
        return namespaceMode;
    }

    @Required
    public void setNamespaceMode(NamespaceCheckingMode namespaceMode)
    {
        this.namespaceMode = namespaceMode;
    }

    public long getCertificateAuthorityUpdateInterval()
    {
        return certificateAuthorityUpdateInterval;
    }

    @Required
    public void setCertificateAuthorityUpdateInterval(long certificateAuthorityUpdateInterval)
    {
        this.certificateAuthorityUpdateInterval = certificateAuthorityUpdateInterval;
    }

    public TimeUnit getCertificateAuthorityUpdateIntervalUnit()
    {
        return certificateAuthorityUpdateIntervalUnit;
    }

    @Required
    public void setCertificateAuthorityUpdateIntervalUnit(TimeUnit unit)
    {
        this.certificateAuthorityUpdateIntervalUnit = unit;
    }

    public void init()
    {
        OCSPParametes ocspParameters = new OCSPParametes(ocspCheckingMode);
        ValidatorParams validatorParams =
                new ValidatorParams(new RevocationParameters(crlCheckingMode, ocspParameters), ProxySupport.ALLOW);
        long updateInterval = certificateAuthorityUpdateIntervalUnit.toMillis(certificateAuthorityUpdateInterval);
        validator = new OpensslCertChainValidator(caPath, true, namespaceMode, updateInterval, validatorParams, false);
    }

    @Override
    protected MoverProtocol createMoverProtocol(ProtocolInfo info) throws Exception
    {
        MoverProtocol moverProtocol;
        if (info instanceof RemoteHttpsDataTransferProtocolInfo) {
            moverProtocol = new RemoteHttpsDataTransferProtocol_1(getCellEndpoint(), validator, secureRandom);
        } else if (info instanceof RemoteHttpDataTransferProtocolInfo) {
            moverProtocol = new RemoteHttpDataTransferProtocol_1(getCellEndpoint());
        } else {
            throw new CacheException(27, "Could not create mover for " + info);
        }
        return moverProtocol;
    }
}
