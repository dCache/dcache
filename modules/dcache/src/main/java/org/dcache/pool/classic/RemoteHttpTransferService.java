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

import com.google.common.base.Splitter;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpsDataTransferProtocolInfo;
import eu.emi.security.authn.x509.OCSPParametes;
import eu.emi.security.authn.x509.ProxySupport;
import eu.emi.security.authn.x509.RevocationParameters;
import eu.emi.security.authn.x509.helpers.ssl.SSLTrustManager;
import eu.emi.security.authn.x509.impl.OpensslCertChainValidator;
import eu.emi.security.authn.x509.impl.ValidatorParams;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.net.ssl.X509TrustManager;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.movers.RemoteHttpDataTransferProtocol;
import org.dcache.pool.movers.RemoteHttpsDataTransferProtocol;
import org.dcache.security.trust.AggregateX509TrustManager;

public class RemoteHttpTransferService extends SecureRemoteTransferService {

    private final List<Runnable> onShutdownTasks = new ArrayList<>();

    private X509TrustManager trustManager;

    @Override
    protected MoverProtocol createMoverProtocol(ProtocolInfo info) throws Exception {
        MoverProtocol moverProtocol;
        if (info instanceof RemoteHttpsDataTransferProtocolInfo) {
            moverProtocol = new RemoteHttpsDataTransferProtocol(getCellEndpoint(),
                  trustManager, secureRandom);
        } else if (info instanceof RemoteHttpDataTransferProtocolInfo) {
            moverProtocol = new RemoteHttpDataTransferProtocol(getCellEndpoint());
        } else {
            throw new CacheException(CacheException.CANNOT_CREATE_MOVER,
                  "Could not create third-party HTTP mover for " + info);
        }
        return moverProtocol;
    }

    @PostConstruct
    public void init() {
        FileSystem defaultFileSystem = FileSystems.getDefault();

        var trustManagers = Splitter.on(':').omitEmptyStrings()
              .splitToList(getCertificateAuthorityPath()).stream()
              .map(defaultFileSystem::getPath)
              .map(this::buildTrustManager)
              .collect(Collectors.toList());

        trustManager = new AggregateX509TrustManager(trustManagers);
    }

    private X509TrustManager buildTrustManager(Path path) {
        var ocspParameters = new OCSPParametes(getOcspCheckingMode());
        var revocationParams = new RevocationParameters(getCrlCheckingMode(), ocspParameters);
        var validatorParams = new ValidatorParams(revocationParams, ProxySupport.ALLOW);
        long updateInterval = getCertificateAuthorityUpdateIntervalUnit().toMillis(
              getCertificateAuthorityUpdateInterval());
        var validator = new OpensslCertChainValidator(path.toString(), true,
              getNamespaceMode(), updateInterval, validatorParams, false);
        onShutdownTasks.add(validator::dispose);
        return new SSLTrustManager(validator);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        onShutdownTasks.forEach(Runnable::run);
    }
}
