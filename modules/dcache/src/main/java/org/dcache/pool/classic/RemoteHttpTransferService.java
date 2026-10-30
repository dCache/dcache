/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015-2025 Deutsches Elektronen-Synchrotron
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

import static com.google.common.base.Preconditions.checkArgument;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpsDataTransferProtocolInfo;
import dmg.cells.nucleus.CellPath;
import eu.emi.security.authn.x509.X509Credential;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import org.apache.http.impl.client.CloseableHttpClient;
import org.dcache.http.tpc.RemoteHttpClientFactory;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.movers.MoverProtocolMover;
import org.dcache.pool.movers.RemoteHttpDataTransferProtocol;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import static dmg.util.Exceptions.meaningfulMessage;

public class RemoteHttpTransferService extends SecureRemoteTransferService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteHttpTransferService.class);

    private static final String USER_AGENT = "dCache/" +
          Version.of(RemoteHttpDataTransferProtocol.class).getVersion();

    @Value("${pool.mover.http-tpc.connections.max}")
    private int maxConnections;

    @Value("${pool.mover.http-tpc.connections.max-per-endpoint}")
    private int maxConnectionsPerEndpoint;

    @Value("${pool.mover.http-tpc.connections.max-idle}")
    private int maxIdle;

    @Value("${pool.mover.http-tpc.connections.max-idle.unit}")
    private TimeUnit maxIdleUnits;

    private RemoteHttpClientFactory clientFactory;

    @Override
    public Mover<?> createMover(ReplicaDescriptor handle, PoolIoFileMessage message,
          CellPath pathToDoor) throws CacheException {
        Mover<?> mover = super.createMover(handle, message, pathToDoor);
        MoverProtocolMover mpm = (MoverProtocolMover) mover;
        ((RemoteHttpDataTransferProtocol) mpm.getMover()).setSubject(message.getSubject());
        return mover;
    }

    @Override
    protected MoverProtocol createMoverProtocol(ProtocolInfo info) throws Exception {
        if (!(info instanceof RemoteHttpDataTransferProtocolInfo)) {
            throw new CacheException(CacheException.CANNOT_CREATE_MOVER,
                  "Could not create third-party HTTP mover for " + info);
        }

        if (info instanceof RemoteHttpsDataTransferProtocolInfo) {
            RemoteHttpsDataTransferProtocolInfo tlsInfo = (RemoteHttpsDataTransferProtocolInfo) info;
            // REVISIT: httpclient supports connection tagging via UserTokenHandler (see
            // HttpClientBuilder#setUserTokenHandler and HttpClientContext#setUserToken), allowing
            // connection reuse even with X.509 credentials.
            if (tlsInfo.hasCredential()) {
                X509Credential credential = tlsInfo.getCredential();
                CloseableHttpClient client = clientFactory.clientFor(credential);

                return new RemoteHttpDataTransferProtocol(client, getTransferLifeCycle()) {
                    @Override
                    protected void afterTransfer() {
                        super.afterTransfer();
                        try {
                            client.close();
                        } catch (IOException e) {
                            LOGGER.warn("Failed to shutdown client cleanly: {}", meaningfulMessage(e));
                        }
                    }
                };
            }
        }

        return new RemoteHttpDataTransferProtocol(clientFactory.sharedClient(),
              getTransferLifeCycle());
    }

    @PostConstruct
    public void init() throws NoSuchAlgorithmException, KeyManagementException {

        checkArgument(maxConnections > 0,
                "'pool.mover.http-tpc.connections.max' must be greater than zero");
        checkArgument(maxConnectionsPerEndpoint > 0,
                "'pool.mover.http-tpc.connections.max-per-endpoint' must be greater than zero");
        checkArgument(maxIdle > 0,
                "'pool.mover.http-tpc.connections.max-idle' must be greater than zero");

        var settings = new RemoteHttpClientFactory.Settings();
        settings.setUserAgent(USER_AGENT);
        settings.setCertificateAuthorityPath(getCertificateAuthorityPath());
        settings.setCertificateAuthorityUpdateInterval(getCertificateAuthorityUpdateInterval());
        settings.setCertificateAuthorityUpdateIntervalUnit(getCertificateAuthorityUpdateIntervalUnit());
        settings.setNamespaceMode(getNamespaceMode());
        settings.setCrlCheckingMode(getCrlCheckingMode());
        settings.setOcspCheckingMode(getOcspCheckingMode());
        settings.setMaxConnections(maxConnections);
        settings.setMaxConnectionsPerEndpoint(maxConnectionsPerEndpoint);
        settings.setMaxIdle(maxIdle);
        settings.setMaxIdleUnits(maxIdleUnits);
        clientFactory = new RemoteHttpClientFactory(settings);
    }

    @Override
    public void shutdown() {
        super.shutdown();

        if (clientFactory != null) {
            clientFactory.close();
        }
    }
}
