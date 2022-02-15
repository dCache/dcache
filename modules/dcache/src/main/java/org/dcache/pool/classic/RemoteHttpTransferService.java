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
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.helpers.ssl.SSLTrustManager;
import eu.emi.security.authn.x509.impl.OpensslCertChainValidator;
import eu.emi.security.authn.x509.impl.ValidatorParams;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.movers.RemoteHttpDataTransferProtocol;
import org.dcache.security.trust.AggregateX509TrustManager;
import org.dcache.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import static com.google.common.base.Preconditions.checkArgument;
import static dmg.util.Exceptions.meaningfulMessage;

public class RemoteHttpTransferService extends SecureRemoteTransferService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteHttpTransferService.class);

    private static final String USER_AGENT = "dCache/" +
          Version.of(RemoteHttpDataTransferProtocol.class).getVersion();

    private static final KeyManager[] NO_KEYMANAGER = new KeyManager[0];

    /**
     * How long the client will wait for the "100 Continue" response when making a PUT request using
     * expect-100.
     */
    private static final Duration EXPECT_100_TIMEOUT = Duration.of(5, ChronoUnit.MINUTES);

    @Value("${pool.mover.http-tpc.connections.max}")
    private int maxConnections;

    @Value("${pool.mover.http-tpc.connections.max-per-endpoint}")
    private int maxConnectionsPerEndpoint;

    @Value("${pool.mover.http-tpc.connections.max-idle}")
    private int maxIdle;

    @Value("${pool.mover.http-tpc.connections.max-idle.unit}")
    private TimeUnit maxIdleUnits;

    private static final RedirectStrategy DROP_AUTHORIZATION_HEADER = new DefaultRedirectStrategy() {

        @Override
        public HttpUriRequest getRedirect(final HttpRequest request,
              final HttpResponse response, final HttpContext context)
              throws ProtocolException {
            HttpUriRequest redirect = super.getRedirect(request, response, context);

            /* If this method returns an HttpUriRequest that has no
             * HTTP headers then the RedirectExec code will copy all
             * the headers from the original request into the
             * HttpUriRequest.   DefaultRedirectStrategy returns such
             * requests under several circumstances.  Therefore, in
             * order to suppress the Authorization header we
             * <em>must</em> ensure the returned request includes
             * headers.
             */
            if (!redirect.headerIterator().hasNext()) {
                redirect.setHeaders(request.getAllHeaders());
            }

            redirect.removeHeaders("Authorization");
            return redirect;
        }
    };


    private final List<Runnable> onShutdownTasks = new ArrayList<>();

    private X509TrustManager trustManager;
    private CloseableHttpClient sharedClient;

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
                SSLContext context = buildSSLContext(credential.getKeyManager());
                CloseableHttpClient client = createClient(context);

                return new RemoteHttpDataTransferProtocol(client) {
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

        return new RemoteHttpDataTransferProtocol(sharedClient);
    }

    @PostConstruct
    public void init() throws NoSuchAlgorithmException, KeyManagementException, KeyManagementException {

        checkArgument(maxConnections > 0,
                "'pool.mover.http-tpc.connections.max' must be greater than zero");
        checkArgument(maxConnectionsPerEndpoint > 0,
                "'pool.mover.http-tpc.connections.max-per-endpoint' must be greater than zero");
        checkArgument(maxIdle > 0,
                "'pool.mover.http-tpc.connections.max-idle' must be greater than zero");

        FileSystem defaultFileSystem = FileSystems.getDefault();

        var trustManagers = Splitter.on(':').omitEmptyStrings()
              .splitToList(getCertificateAuthorityPath()).stream()
              .map(defaultFileSystem::getPath)
              .map(this::buildTrustManager)
              .collect(Collectors.toList());

        trustManager = new AggregateX509TrustManager(trustManagers);

        SSLContext context = buildSSLContext(null);
        sharedClient = createClient(context);
    }

    private CloseableHttpClient createClient(SSLContext context) {
        return HttpClients.custom()
                .setUserAgent(USER_AGENT)
                .evictIdleConnections(maxIdle, maxIdleUnits)
                .setMaxConnPerRoute(maxConnectionsPerEndpoint)
                .setMaxConnTotal(maxConnections)
                .setRequestExecutor(new HttpRequestExecutor((int) EXPECT_100_TIMEOUT.toMillis()))
                .setRedirectStrategy(DROP_AUTHORIZATION_HEADER)
                .setSSLContext(context)
                .build();
    }

    protected SSLContext buildSSLContext(@Nullable KeyManager keyManager)
            throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext context = SSLContext.getInstance("TLS");
        X509TrustManager[] trustManagers = {trustManager};
        KeyManager[] keyManagers = keyManager == null ? NO_KEYMANAGER : new KeyManager[]{keyManager};
        context.init(keyManagers, trustManagers, secureRandom);
        return context;
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

        if (sharedClient != null) {
            try {
                sharedClient.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to shut down HTTP client cleanly: {}", meaningfulMessage(e));
            }
        }
    }
}
