/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015-2020 Deutsches Elektronen-Synchrotron
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

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;

import dmg.cells.nucleus.CDC;

import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.movers.RemoteGsiftpTransferProtocol;
import org.dcache.ssl.CanlContextFactory;
import org.dcache.ssl.SslContextFactory;
import org.dcache.util.PortRange;

public class RemoteGsiftpTransferService extends SecureRemoteTransferService
{
    private CanlContextFactory sslContextFactory;
    private String[] bannedCiphers;
    private PortRange portRange;

    public String[] getBannedCiphers()
    {
        return bannedCiphers;
    }

    public void setBannedCiphers(String[] bannedCiphers)
    {
        this.bannedCiphers = bannedCiphers;
    }

    public PortRange getPortRange()
    {
        return portRange;
    }

    public void setPortRange(PortRange portRange)
    {
        this.portRange = portRange;
    }

    @Override
    protected MoverProtocol createMoverProtocol(ProtocolInfo info) throws Exception
    {
        MoverProtocol moverProtocol;
        if (info instanceof RemoteGsiftpTransferProtocolInfo) {
            moverProtocol = new RemoteGsiftpTransferProtocol(getCellEndpoint(), portRange, bannedCiphers, getContextFactory());
        } else {
            throw new CacheException(CacheException.CANNOT_CREATE_MOVER,
                    "Could not create third-party GSIFTP mover for " + info);
        }
        return moverProtocol;
    }

    private synchronized SslContextFactory getContextFactory()
    {
        if (sslContextFactory == null) {
            sslContextFactory =
                    CanlContextFactory.custom()
                            .withCertificateAuthorityPath(getCertificateAuthorityPath())
                            .withCertificateAuthorityUpdateInterval(getCertificateAuthorityUpdateInterval(),
                                                                    getCertificateAuthorityUpdateIntervalUnit())
                            .withCrlCheckingMode(getCrlCheckingMode())
                            .withOcspCheckingMode(getOcspCheckingMode())
                            .withNamespaceMode(getNamespaceMode())
                            .withLazy(false)
                            .withLoggingContext(new CDC()::restore)
                            .build();
        }
        return sslContextFactory;
    }
}
