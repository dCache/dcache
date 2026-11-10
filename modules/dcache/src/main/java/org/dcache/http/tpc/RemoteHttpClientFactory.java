/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2025 Deutsches Elektronen-Synchrotron
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
package org.dcache.http.tpc;

import static dmg.util.Exceptions.meaningfulMessage;

import com.google.common.base.Splitter;
import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import eu.emi.security.authn.x509.OCSPParametes;
import eu.emi.security.authn.x509.ProxySupport;
import eu.emi.security.authn.x509.RevocationParameters;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.helpers.ssl.EnforcingNameMismatchCallback;
import eu.emi.security.authn.x509.helpers.ssl.SSLTrustManagerWithHostnameChecking;
import eu.emi.security.authn.x509.impl.OpensslCertChainValidator;
import eu.emi.security.authn.x509.impl.ValidatorParams;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedTrustManager;
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
import org.dcache.security.trust.AggregateX509TrustManager;
import org.dcache.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the HTTP clients used for HTTP third-party transfers.  Remote servers are authenticated
 * against the trust anchors found in the certificate authority path.  Optionally, a client may
 * authenticate with an X.509 credential.
 */
public class RemoteHttpClientFactory implements AutoCloseable {

    /**
     * Configuration of the trust validation and connection pool.
     */
    public static class Settings {

        private String userAgent = "dCache/" + Version.of(RemoteHttpClientFactory.class).getVersion();
        private String certificateAuthorityPath;
        private long certificateAuthorityUpdateInterval;
        private TimeUnit certificateAuthorityUpdateIntervalUnit;
        private NamespaceCheckingMode namespaceMode;
        private CrlCheckingMode crlCheckingMode;
        private OCSPCheckingMode ocspCheckingMode;
        private int maxConnections;
        private int maxConnectionsPerEndpoint;
        private int maxIdle;
        private TimeUnit maxIdleUnits;

        public String getUserAgent() {
            return userAgent;
        }

        public void setUserAgent(String userAgent) {
            this.userAgent = userAgent;
        }

        public String getCertificateAuthorityPath() {
            return certificateAuthorityPath;
        }

        public void setCertificateAuthorityPath(String certificateAuthorityPath) {
            this.certificateAuthorityPath = certificateAuthorityPath;
        }

        public long getCertificateAuthorityUpdateInterval() {
            return certificateAuthorityUpdateInterval;
        }

        public void setCertificateAuthorityUpdateInterval(long certificateAuthorityUpdateInterval) {
            this.certificateAuthorityUpdateInterval = certificateAuthorityUpdateInterval;
        }

        public TimeUnit getCertificateAuthorityUpdateIntervalUnit() {
            return certificateAuthorityUpdateIntervalUnit;
        }

        public void setCertificateAuthorityUpdateIntervalUnit(TimeUnit certificateAuthorityUpdateIntervalUnit) {
            this.certificateAuthorityUpdateIntervalUnit = certificateAuthorityUpdateIntervalUnit;
        }

        public NamespaceCheckingMode getNamespaceMode() {
            return namespaceMode;
        }

        public void setNamespaceMode(NamespaceCheckingMode namespaceMode) {
            this.namespaceMode = namespaceMode;
        }

        public CrlCheckingMode getCrlCheckingMode() {
            return crlCheckingMode;
        }

        public void setCrlCheckingMode(CrlCheckingMode crlCheckingMode) {
            this.crlCheckingMode = crlCheckingMode;
        }

        public OCSPCheckingMode getOcspCheckingMode() {
            return ocspCheckingMode;
        }

        public void setOcspCheckingMode(OCSPCheckingMode ocspCheckingMode) {
            this.ocspCheckingMode = ocspCheckingMode;
        }

        public int getMaxConnections() {
            return maxConnections;
        }

        public void setMaxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
        }

        public int getMaxConnectionsPerEndpoint() {
            return maxConnectionsPerEndpoint;
        }

        public void setMaxConnectionsPerEndpoint(int maxConnectionsPerEndpoint) {
            this.maxConnectionsPerEndpoint = maxConnectionsPerEndpoint;
        }

        public int getMaxIdle() {
            return maxIdle;
        }

        public void setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
        }

        public TimeUnit getMaxIdleUnits() {
            return maxIdleUnits;
        }

        public void setMaxIdleUnits(TimeUnit maxIdleUnits) {
            this.maxIdleUnits = maxIdleUnits;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteHttpClientFactory.class);

    private static final KeyManager[] NO_KEYMANAGER = new KeyManager[0];

    /**
     * How long the client will wait for the "100 Continue" response when making a PUT request using
     * expect-100.
     */
    private static final Duration EXPECT_100_TIMEOUT = Duration.of(5, ChronoUnit.MINUTES);

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

    private final Settings settings;
    private final SecureRandom secureRandom = new SecureRandom();
    private final List<Runnable> onShutdownTasks = new ArrayList<>();
    private final X509TrustManager trustManager;
    private final CloseableHttpClient sharedClient;

    public RemoteHttpClientFactory(Settings settings)
          throws NoSuchAlgorithmException, KeyManagementException {
        this.settings = settings;

        FileSystem defaultFileSystem = FileSystems.getDefault();

        var trustManagers = Splitter.on(':').omitEmptyStrings()
              .splitToList(settings.getCertificateAuthorityPath()).stream()
              .map(defaultFileSystem::getPath)
              .map(this::buildTrustManager)
              .collect(Collectors.toList());

        trustManager = new AggregateX509TrustManager(trustManagers);
        sharedClient = createClient(buildSSLContext(null));
    }

    /**
     * A client that may be used concurrently by many transfers and is closed by this factory.
     * Callers must not close it.
     */
    public CloseableHttpClient sharedClient() {
        return sharedClient;
    }

    /**
     * Create a client that authenticates with the supplied credential.  The caller must close the
     * client.
     */
    public CloseableHttpClient clientFor(X509Credential credential)
          throws NoSuchAlgorithmException, KeyManagementException {
        return createClient(buildSSLContext(credential.getKeyManager()));
    }

    private CloseableHttpClient createClient(SSLContext context) {
        return HttpClients.custom()
              .setUserAgent(settings.getUserAgent())
              .evictIdleConnections(settings.getMaxIdle(), settings.getMaxIdleUnits())
              .setMaxConnPerRoute(settings.getMaxConnectionsPerEndpoint())
              .setMaxConnTotal(settings.getMaxConnections())
              .setRequestExecutor(new HttpRequestExecutor((int) EXPECT_100_TIMEOUT.toMillis()))
              .setRedirectStrategy(DROP_AUTHORIZATION_HEADER)
              .setSSLContext(context)
              .build();
    }

    private SSLContext buildSSLContext(@Nullable KeyManager keyManager)
          throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext context = SSLContext.getInstance("TLS");
        X509TrustManager[] trustManagers = {trustManager};
        KeyManager[] keyManagers = keyManager == null ? NO_KEYMANAGER : new KeyManager[]{keyManager};
        context.init(keyManagers, trustManagers, secureRandom);
        return context;
    }

    private X509ExtendedTrustManager buildTrustManager(Path path) {
        var ocspParameters = new OCSPParametes(settings.getOcspCheckingMode());
        var revocationParams = new RevocationParameters(settings.getCrlCheckingMode(), ocspParameters);
        var validatorParams = new ValidatorParams(revocationParams, ProxySupport.ALLOW);
        long updateInterval = settings.getCertificateAuthorityUpdateIntervalUnit().toMillis(
              settings.getCertificateAuthorityUpdateInterval());
        var validator = new OpensslCertChainValidator(path.toString(), true,
              settings.getNamespaceMode(), updateInterval, validatorParams, false);
        onShutdownTasks.add(validator::dispose);
        return new SSLTrustManagerWithHostnameChecking(validator,
              new EnforcingNameMismatchCallback());
    }

    @Override
    public void close() {
        onShutdownTasks.forEach(Runnable::run);

        try {
            sharedClient.close();
        } catch (IOException e) {
            LOGGER.warn("Failed to shut down HTTP client cleanly: {}", meaningfulMessage(e));
        }
    }
}
