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

import static com.google.common.base.Strings.padStart;
import static com.google.common.collect.Maps.uniqueIndex;
import static diskCacheV111.util.ThirdPartyTransferFailedCacheException.checkThirdPartyTransferSuccessful;
import static dmg.util.Exceptions.getMessageWithCauses;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.dcache.util.ByteUnit.GiB;
import static org.dcache.util.ByteUnit.MiB;
import static org.dcache.util.Exceptions.messageOrClassName;
import static org.dcache.util.Strings.describeSize;
import static org.dcache.util.Strings.toThreeSigFig;
import static org.dcache.util.TimeUtils.describeDuration;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ThirdPartyTransferFailedCacheException;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpInetConnection;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.ConnectionShutdownException;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.dcache.auth.OpenIdCredentialRefreshable;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Checksums;
import org.dcache.util.HttpExtHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The client side of a single HTTP third-party transfer: a GET, or a PUT followed by verification
 * of the remote copy.  The protocol details (RFC 3230 / RFC 9530 digests, redirects, retries,
 * authorization and checksum verification) are independent of where the data is read from or
 * written to.  This class is used by both the pool's remote-HTTP mover and by the WebDAV door when
 * it proxies a third-party transfer.
 * <p>
 * An instance describes one transfer and must not be shared between transfers.
 */
public class RemoteHttpTransferClient {

    /**
     * Receives the body of a successful GET response.
     */
    @FunctionalInterface
    public interface BodyReceiver {

        void receive(HttpEntity entity) throws IOException, InterruptedException;
    }

    /**
     * Notified of the length of a GET response entity.
     */
    @FunctionalInterface
    public interface ContentLengthListener {

        void contentLength(long length) throws IOException;
    }

    /**
     * Progress information used to enrich error messages.
     */
    public interface Progress {

        long getBytesTransferred();

        long getSize() throws IOException;
    }

    private enum HeaderFlags {
        /**
         * Do not include any Authorization request header.
         */
        NO_AUTHORIZATION_HEADER
    }

    private static final Set<HeaderFlags> REDIRECTED_REQUEST
          = EnumSet.of(HeaderFlags.NO_AUTHORIZATION_HEADER);

    private static final Set<HeaderFlags> INITIAL_REQUEST
          = EnumSet.noneOf(HeaderFlags.class);

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteHttpTransferClient.class);

    /**
     * Maximum time to wait when establishing a connection.
     */
    private static final int CONNECTION_TIMEOUT = (int) TimeUnit.MINUTES.toMillis(1);

    /**
     * Minimum time to wait for next packet from remote server.  This is mostly used to allow for a
     * server that accepts a request but takes a non-trivial amount of time to start its reply.  As
     * we cannot easily create a check for how long the remote server takes to provide the first
     * byte of its response, the socket timeout is used instead.
     */
    private static final int SOCKET_TIMEOUT = (int) TimeUnit.MINUTES.toMillis(1);

    /**
     * Expected maximum delay all post-processing files will experience, in milliseconds.
     */
    private static final long POST_PROCESSING_OFFSET = 60_000;

    /**
     * Expected minimum (effective) internal IO bandwidth of the remote server, in bytes per
     * millisecond.  This is used to estimate how long any file post-processing (like checksum
     * calculation) will take.
     */
    private static final double POST_PROCESSING_BANDWIDTH = MiB.toBytes(10) / 1_000.0;

    /**
     * Number of milliseconds between successive requests.
     */
    private static final long DELAY_BETWEEN_REQUESTS = 5_000;

    /**
     * A guess on how long a GET request might take.
     * <p>
     * Note, we do not know the file size when making a GET request, so the file size here is a
     * guess, based on a likely value.  Therefore, the duration may be insufficient if the remote
     * server is loaded and the file is larger.
     */
    private static final long GET_RETRY_DURATION = maxRetryDuration(GiB.toBytes(2L));
    private static final String GET_RETRY_DURATION_DESCRIPTION = describeDuration(
          GET_RETRY_DURATION, MILLISECONDS);

    /**
     * Maximum number of redirections to follow. Note that, although RFC 2068 section 10.3
     * recommends a maximum of 5, both firefox and webkit currently limit (by default) to 20
     * redirections.
     */
    private static final int MAX_REDIRECTIONS = 20;

    private static final String AUTH_BEARER = "Bearer ";

    private static final String SCITAG_HEADER = "SciTag";

    private static final String WANT_DIGEST_VALUE = Checksums.buildGenericWantDigest(
          Checksums.RfcType.RFC3230);
    private static final String WANT_REPR_DIGEST_VALUE = Checksums.buildGenericWantDigest(
          Checksums.RfcType.RFC9530);
    private static final Pattern MATCH_HEXADECIMAL = Pattern.compile("[0-9a-f]+");

    private static final Map<String, ChecksumType> OC_CHECKSUM_TYPE = Map.of(
          "MD5", ChecksumType.MD5_TYPE,
          "SHA1", ChecksumType.SHA1,
          "Adler32", ChecksumType.ADLER32);

    private final CloseableHttpClient _client;
    private final RemoteHttpDataTransferProtocolInfo _info;
    private final Runnable _onConnected;

    @GuardedBy("this")
    private HttpClientContext _context;

    private volatile Long _expectedTransferSize;

    /**
     * @param client      the HTTP client used to make all requests
     * @param info        describes the remote resource, credentials and headers
     * @param onConnected called after the first response headers of a request have been received,
     *                    when the connection's endpoints are known
     */
    public RemoteHttpTransferClient(CloseableHttpClient client,
          RemoteHttpDataTransferProtocolInfo info, Runnable onConnected) {
        _client = requireNonNull(client);
        _info = requireNonNull(info);
        _onConnected = requireNonNull(onConnected);
    }

    /**
     * The expected number of bytes of the transfer, if known.
     */
    public Long getBytesExpected() {
        return _expectedTransferSize;
    }

    /**
     * Download the remote file.
     *
     * @param integrityChecker receives all checksums that the remote server reports
     * @param receiver         consumes the response entity
     * @param progress         reports how much data has been transferred
     * @param onContentLength  called with the length of the entity, if the server reported one
     */
    public void get(Consumer<Checksum> integrityChecker, BodyReceiver receiver,
          Progress progress, ContentLengthListener onContentLength)
          throws ThirdPartyTransferFailedCacheException {
        boolean haveReceivedChecksum;

        long deadline = System.currentTimeMillis() + GET_RETRY_DURATION;

        HttpClientContext context = storeContext(new HttpClientContext());
        try {
            try (CloseableHttpResponse response = doGet(context, deadline)) {
                haveReceivedChecksum = acceptChecksums(response, integrityChecker);

                HttpEntity entity = response.getEntity();
                if (entity == null) {
                    throw new ThirdPartyTransferFailedCacheException(
                          "GET response contains no content");
                }

                long length = entity.getContentLength();
                if (length > 0) {
                    onContentLength.contentLength(length);
                }
                if (response.getStatusLine() != null
                      && response.getStatusLine().getStatusCode() < 300 && length > -1) {
                    _expectedTransferSize = length;
                }
                receiver.receive(entity);
            } catch (SocketTimeoutException e) {
                String message = "socket timeout on GET (received "
                      + describeSize(progress.getBytesTransferred()) + " of data; "
                      + describeSize(e.bytesTransferred) + " pending)";
                if (e.getMessage() != null) {
                    message += ": " + e.getMessage();
                }
                throw new ThirdPartyTransferFailedCacheException(message, e);
            } catch (IOException e) {
                throw new ThirdPartyTransferFailedCacheException(messageOrClassName(e), e);
            } catch (InterruptedException e) {
                throw new ThirdPartyTransferFailedCacheException("shutting down", e);
            }
        } catch (ThirdPartyTransferFailedCacheException e) {
            List<URI> redirections = context.getRedirectLocations();
            if (redirections != null && !redirections.isEmpty()) {
                StringBuilder message = new StringBuilder(e.getMessage());
                message.append("; redirects ").append(redirections);
                throw new ThirdPartyTransferFailedCacheException(message.toString(), e.getCause());
            } else {
                throw e;
            }
        }

        // Work-around for Dynafed, which only supplies checksum information on
        // HEAD requests.
        if (!haveReceivedChecksum && _info.isVerificationRequired()) {
            HttpHead head = buildHeadRequest(deadline);
            head.addHeader(HttpExtHeader.WANT_DIGEST, WANT_DIGEST_VALUE);
            head.addHeader(HttpExtHeader.WANT_REPR_DIGEST, WANT_REPR_DIGEST_VALUE);

            try {
                try (CloseableHttpResponse response = _client.execute(head)) {
                    haveReceivedChecksum = acceptChecksums(response, integrityChecker);
                    checkThirdPartyTransferSuccessful(!haveReceivedChecksum,
                          "no checksum in HEAD response");
                }
            } catch (IOException e) {
                throw new ThirdPartyTransferFailedCacheException(
                      "HEAD request failed: " + messageOrClassName(e), e);
            }
        }
    }

    /**
     * Upload a file, then verify the remote copy.  If the upload or verification fails, an attempt
     * is made to delete the remote file.
     *
     * @param entity         supplies the request body.  It is asked for once per PUT request
     *                       (i.e., once per redirection).
     * @param progress       reports how much data has been transferred
     * @param fileSize       the size of the file
     * @param knownChecksums the checksums dCache knows for this file
     */
    public void putAndVerify(Supplier<HttpEntity> entity, Progress progress, long fileSize,
          Optional<Set<Checksum>> knownChecksums) throws ThirdPartyTransferFailedCacheException {
        put(entity, progress);

        try {
            verifyRemoteFile(fileSize, knownChecksums);
        } catch (ThirdPartyTransferFailedCacheException e) {
            deleteRemoteFile(e.getMessage());
            throw new ThirdPartyTransferFailedCacheException("verification " +
                  "failed: " + e.getMessage());
        }
    }

    /**
     * The address of the remote endpoint of the most recent request, if known.
     */
    public Optional<InetSocketAddress> remoteAddress() {
        return address(true);
    }

    /**
     * The local address of the most recent request's connection, if known.
     */
    public Optional<InetSocketAddress> localAddress() {
        return address(false);
    }

    private synchronized HttpClientContext storeContext(HttpClientContext context) {
        _context = context;
        return context;
    }

    private synchronized HttpClientContext getContext() {
        return _context;
    }

    private Optional<InetSocketAddress> address(boolean remote) {
        HttpContext context = getContext();
        if (context == null) {
            LOGGER.debug("No HttpContext value");
            return Optional.empty();
        }

        Object conn = context.getAttribute(HttpCoreContext.HTTP_CONNECTION);
        if (conn == null) {
            LOGGER.debug("HTTP_CONNECTION is null");
            return Optional.empty();
        }

        if (!(conn instanceof HttpInetConnection)) {
            throw new RuntimeException("HTTP_CONNECTION has unexpected type: "
                  + conn.getClass().getCanonicalName());
        }

        HttpInetConnection inetConn = (HttpInetConnection) conn;
        if (!inetConn.isOpen()) {
            LOGGER.debug("HttpConnection is no longer open");
            return Optional.empty();
        }

        try {
            InetAddress addr = remote ? inetConn.getRemoteAddress() : inetConn.getLocalAddress();
            if (addr == null) {
                LOGGER.debug("HttpInetConnection is not connected");
                return Optional.empty();
            }

            int port = remote ? inetConn.getRemotePort() : inetConn.getLocalPort();
            return Optional.of(new InetSocketAddress(addr, port));
        } catch (ConnectionShutdownException e) {
            LOGGER.warn("HTTP_CONNECTION has unexpectedly disconnected");
            // Perhaps a race condition here?  Hey ho.
            return Optional.empty();
        }
    }

    private Optional<Checksum> decodeContentMd5(HttpResponse response) {
        String headerValue = headerValue(response, "Content-MD5");
        if (headerValue == null) {
            return Optional.empty();
        }

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_PARTIAL_CONTENT) {
            // Here, we assume that if the remote server replies with 206 then the entity is not
            // the complete file, which would mean that any Content-MD5 header value is not useful.
            return Optional.empty();
        }

        String contentEncoding = headerValue(response, "Content-Encoding");
        if (contentEncoding != null && !contentEncoding.equals("identity")) {
            // The entity is compressed (or otherwise encoded), which means the Content-MD5, if
            // present, describes the encoded version and not the decoded (raw) version.  Therefore,
            // the checksum is not useful for verifying data integrity within dCache.
            return Optional.empty();
        }

        String contentRange = headerValue(response, "Content-Range");
        if (contentRange != null) {
            // Here, we assume that the presence of any "Content-Range" implies a partial response.
            // We could do more: parse the response and check whether the range represents the
            // whole file.
            return Optional.empty();
        }

        if (!MATCH_HEXADECIMAL.matcher(headerValue).matches()) {
            LOGGER.warn("Invalid Content-MD5 header value \"{}\"", headerValue);
            return Optional.empty();
        }
        return Optional.ofNullable(headerValue).map(v -> new Checksum(ChecksumType.MD5_TYPE, v));
    }

    private Optional<Checksum> decodeOcChecksum(HttpResponse response) {
        String headerValue = headerValue(response, "OC-Checksum");
        if (headerValue == null) {
            return Optional.empty();
        }

        int idx = headerValue.indexOf(':');
        if (idx == -1) {
            LOGGER.warn("Remote server responded with a bad OC-Checksum value \"{}\"",
                  headerValue);
            return Optional.empty();
        }

        String typeLabel = headerValue.substring(0, idx);
        ChecksumType type = OC_CHECKSUM_TYPE.get(typeLabel);
        if (type == null) {
            LOGGER.warn("Remote server responded with unknown OC-Checksum algorithm \"{}\"",
                  headerValue);
            return Optional.empty();
        }

        String value = headerValue.substring(idx + 1);
        if (!MATCH_HEXADECIMAL.matcher(value).matches()) {
            LOGGER.warn("Invalid OC-Checksum header {} value \"{}\"", typeLabel, value);
            return Optional.empty();
        }

        value = padStart(value, type.getNibbles(), '0');
        return Optional.of(new Checksum(type, value));
    }

    /**
     * Extract all checksums the remote server reported in a response.
     */
    public Set<Checksum> checksumsFromResponse(HttpResponse response) {
        String rfc3230 = headerValue(response, HttpExtHeader.DIGEST);
        String rfc9530 = headerValue(response, HttpExtHeader.REPR_DIGEST);
        Set<Checksum> digestChecksums = (rfc9530 != null)
              ? Checksums.decodeRfc(rfc9530, Checksums.RfcType.RFC9530)
              : Checksums.decodeRfc(rfc3230, Checksums.RfcType.RFC3230);

        Optional<Checksum> contentMd5Checksum = decodeContentMd5(response);

        Optional<Checksum> ocChecksum = decodeOcChecksum(response);

        if (contentMd5Checksum.isEmpty() && ocChecksum.isEmpty()) {
            return digestChecksums;
        }

        Set<Checksum> combined = new HashSet<>();
        combined.addAll(digestChecksums);
        contentMd5Checksum.ifPresent(combined::add);
        ocChecksum.ifPresent(combined::add);
        return combined;
    }

    private boolean acceptChecksums(HttpResponse response, Consumer<Checksum> integrityChecker) {
        Set<Checksum> checksums = checksumsFromResponse(response);
        checksums.forEach(integrityChecker);
        return !checksums.isEmpty();
    }

    private HttpGet buildGetRequest(long deadline) {
        HttpGet get = new HttpGet(_info.getUri());
        get.addHeader(HttpExtHeader.WANT_DIGEST, WANT_DIGEST_VALUE);
        get.addHeader(HttpExtHeader.WANT_REPR_DIGEST, WANT_REPR_DIGEST_VALUE);
        addHeadersToRequest(get, INITIAL_REQUEST);

        int timeLeftBeforeDeadline = (int) (deadline - System.currentTimeMillis());
        int socketTimeout = Math.max(SOCKET_TIMEOUT, timeLeftBeforeDeadline);

        get.setConfig(RequestConfig.custom()
              .setConnectTimeout(CONNECTION_TIMEOUT)
              .setSocketTimeout(socketTimeout)
              .build());
        return get;
    }

    private CloseableHttpResponse doGet(HttpContext context, long deadline) throws IOException,
          ThirdPartyTransferFailedCacheException, InterruptedException {
        HttpGet get = buildGetRequest(deadline);
        CloseableHttpResponse response = _client.execute(get, context);

        _onConnected.run();

        boolean isSuccessful = false;
        try {
            while (shouldRetry(response) && System.currentTimeMillis() < deadline) {
                Thread.sleep(DELAY_BETWEEN_REQUESTS);

                response.close();
                get = buildGetRequest(deadline);
                response = _client.execute(get);
            }

            int statusCode = response.getStatusLine().getStatusCode();
            String reason = response.getStatusLine().getReasonPhrase();

            checkThirdPartyTransferSuccessful(!shouldRetry(response),
                  "remote server not ready for GET request after %s: %d %s",
                  GET_RETRY_DURATION_DESCRIPTION, statusCode, reason);

            checkThirdPartyTransferSuccessful(statusCode == HttpStatus.SC_OK,
                  "rejected GET: %d %s", statusCode, reason);

            isSuccessful = true;
        } finally {
            if (!isSuccessful) {
                response.close();
            }
        }

        return response;
    }

    private static boolean shouldRetry(HttpResponse response) {
        // DPM will return 202 for GET or HEAD with Want-Digest if it's still
        // calculating the checksum.
        return response.getStatusLine().getStatusCode() == HttpStatus.SC_ACCEPTED;
    }

    private void put(Supplier<HttpEntity> entity, Progress progress)
          throws ThirdPartyTransferFailedCacheException {
        URI location = _info.getUri();
        List<URI> redirections = null;
        HttpClientContext context = storeContext(new HttpClientContext());

        try {
            for (int redirectionCount = 0; redirectionCount < MAX_REDIRECTIONS;
                  redirectionCount++) {
                HttpPut put = buildPutRequest(location, entity.get(),
                      redirectionCount > 0 ? REDIRECTED_REQUEST : INITIAL_REQUEST);

                try (CloseableHttpResponse response = _client.execute(put, context)) {
                    _onConnected.run();
                    StatusLine status = response.getStatusLine();
                    switch (status.getStatusCode()) {
                        case 200: /* OK (not actually a valid response from PUT) */
                        case 201: /* Created */
                        case 204: /* No Content */
                        case 205: /* Reset Content */
                            return;

                        case 300: /* Multiple Choice */
                        case 301: /* Moved Permanently */
                        case 302: /* Found (REVISIT: should we treat this as an error?) */
                        case 307: /* Temporary Redirect */
                        case 308: /* Permanent Redirect */
                            String locationHeader = response.getFirstHeader("Location").getValue();
                            if (locationHeader == null) {
                                throw new ThirdPartyTransferFailedCacheException(
                                      "missing Location in PUT response "
                                            + status.getStatusCode() + " "
                                            + status.getReasonPhrase());
                            }

                            try {
                                location = URI.create(locationHeader);
                            } catch (IllegalArgumentException e) {
                                throw new ThirdPartyTransferFailedCacheException(
                                      "invalid Location " +
                                            locationHeader + " in PUT response "
                                            + status.getStatusCode() + " "
                                            + status.getReasonPhrase()
                                            + ": " + e.getMessage());
                            }
                            if (redirections == null) {
                                redirections = new ArrayList<>();
                            }
                            redirections.add(location);
                            break;

                        /* Treat all other responses as a failure. */
                        default:
                            throw new ThirdPartyTransferFailedCacheException("rejected PUT: "
                                  + status.getStatusCode() + " " + status.getReasonPhrase());
                    }
                } catch (ConnectException e) {
                    throw new ThirdPartyTransferFailedCacheException("connection failed for PUT: "
                          + messageOrClassName(e), e);
                } catch (ClientProtocolException e) {
                    // Sometimes the real error is wrapped within a ClientProtocolException,
                    // which adds no useful information.  This we skip here.
                    Throwable t = e.getMessage() == null && e.getCause() != null ? e.getCause() : e;
                    StringBuilder message = new StringBuilder("failed to send PUT request: ")
                          .append(getMessageWithCauses(t));
                    if (progress.getBytesTransferred() != 0) {
                        message.append("; after sending ")
                              .append(describeSize(progress.getBytesTransferred()));
                        try {
                            String percent = toThreeSigFig(
                                  100 * progress.getBytesTransferred() / (double) progress.getSize(),
                                  1000);
                            message.append(" (").append(percent).append("%)");
                        } catch (IOException io) {
                            LOGGER.warn("failed to discover file size: {}", messageOrClassName(io));
                        }
                    }
                    throw new ThirdPartyTransferFailedCacheException(message.toString(), e);
                } catch (IOException e) {
                    throw new ThirdPartyTransferFailedCacheException(
                          "problem sending data: " + messageOrClassName(e), e);
                }
            }
        } catch (ThirdPartyTransferFailedCacheException e) {
            if (redirections != null) {
                throw new ThirdPartyTransferFailedCacheException(e.getMessage()
                      + "; redirections " + redirections, e.getCause());
            } else {
                throw e;
            }
        }

        throw new ThirdPartyTransferFailedCacheException("exceeded maximum"
              + " number of redirections: " + redirections);
    }

    private HttpPut buildPutRequest(URI location, HttpEntity entity, Set<HeaderFlags> flags) {
        HttpPut put = new HttpPut(location);
        put.setConfig(RequestConfig.custom()
              .setConnectTimeout(CONNECTION_TIMEOUT)
              .setExpectContinueEnabled(true)
              .setSocketTimeout(0)
              .build());
        addHeadersToRequest(put, flags);
        put.setEntity(entity);

        long size = entity.getContentLength();
        if (size != -1) {
            _expectedTransferSize = size;
        }

        // FIXME add SO_KEEPALIVE setting

        return put;
    }

    /**
     * How long to retry a GET or HEAD request for a file with given size.
     *
     * @param fileSize The file's size, in bytes.
     * @return the maximum retry duration, in milliseconds.
     */
    private static long maxRetryDuration(long fileSize) {
        /*
         * We estimate how long any post-processing will take based on a
         * linear model.  The model is:
         *
         *     t_max = alpha + S / beta
         *
         * where t_max is the maximum time post-processing is expected to take,
         * S is the file's size,  alpha is the fixed time that all files require
         * and beta is the effective IO bandwidth within the remote server.
         */
        return POST_PROCESSING_OFFSET + (long) (fileSize / POST_PROCESSING_BANDWIDTH);
    }

    private void verifyRemoteFile(long fileSize, Optional<Set<Checksum>> knownChecksums)
          throws ThirdPartyTransferFailedCacheException {
        boolean isFirstAttempt = true;

        long t_max = maxRetryDuration(fileSize);
        long deadline = System.currentTimeMillis() + t_max;

        try {
            while (System.currentTimeMillis() < deadline) {
                long sleepFor = Math.min(deadline - System.currentTimeMillis(),
                      DELAY_BETWEEN_REQUESTS);
                if (!isFirstAttempt && sleepFor > 0) {
                    Thread.sleep(sleepFor);
                }
                isFirstAttempt = false;

                HttpClientContext context = storeContext(new HttpClientContext());
                HttpHead head = buildHeadRequest(deadline);
                Optional<String> wantDigest = buildWantDigest(knownChecksums);
                wantDigest.ifPresent(v -> head.addHeader(HttpExtHeader.WANT_DIGEST, v));

                if (wantDigest.isPresent() && (wantDigest.get().equals("sha-512")
                      || wantDigest.get().equals("sha-256"))) {
                    head.addHeader(HttpExtHeader.WANT_REPR_DIGEST, wantDigest.get());
                }

                try {
                    try (CloseableHttpResponse response = _client.execute(head, context)) {
                        StatusLine status = response.getStatusLine();

                        if (status.getStatusCode() >= 300) {
                            checkThirdPartyTransferSuccessful(!_info.isVerificationRequired(),
                                  "rejected HEAD: %d %s", status.getStatusCode(),
                                  status.getReasonPhrase());
                            return;
                        }

                        if (shouldRetry(response)) {
                            continue;
                        }

                        OptionalLong contentLengthHeader = contentLength(response);

                        if (contentLengthHeader.isPresent()) {
                            long contentLength = contentLengthHeader.getAsLong();
                            checkThirdPartyTransferSuccessful(contentLength == fileSize,
                                  "HEAD Content-Length (%d) does not match file size (%d)",
                                  contentLength, fileSize);
                        } else {
                            LOGGER.debug("HEAD response did not contain Content-Length");
                        }

                        Set<Checksum> remoteChecksums = checksumsFromResponse(response);
                        checkChecksums(remoteChecksums, knownChecksums);
                        return;
                    } catch (IOException e) {
                        throw new ThirdPartyTransferFailedCacheException("failed to " +
                              "connect to server: " + e.toString(), e);
                    }
                } catch (ThirdPartyTransferFailedCacheException e) {
                    List<URI> redirections = context.getRedirectLocations();
                    if (redirections != null && !redirections.isEmpty()) {
                        throw new ThirdPartyTransferFailedCacheException(
                              e.getMessage() + "; redirections " + redirections, e.getCause());
                    } else {
                        throw e;
                    }

                }
            }
        } catch (InterruptedException e) {
            throw new ThirdPartyTransferFailedCacheException("shutting down", e);
        }

        throw new ThirdPartyTransferFailedCacheException("remote server failed " +
              "to provide length after " + describeDuration(GET_RETRY_DURATION, MILLISECONDS));
    }

    private static Optional<String> buildWantDigest(Optional<Set<Checksum>> checksums) {
        return checksums.map(Checksums::asWantDigest)
              .filter(Optional::isPresent)
              .map(Optional::get);
    }

    private HttpHead buildHeadRequest(long deadline) {
        HttpHead head = new HttpHead(_info.getUri());

        int timeLeftBeforeDeadline = (int) (deadline - System.currentTimeMillis());
        int socketTimeout = Math.max(SOCKET_TIMEOUT, timeLeftBeforeDeadline);

        head.setConfig(RequestConfig.custom()
              .setConnectTimeout(CONNECTION_TIMEOUT)
              .setSocketTimeout(socketTimeout)

              /* When making a HEAD request, we want to use
               * the 'Content-Length' response header to
               * learn of the file's size.  This only works
               * if the remote server does not compress the
               * content or otherwise encode it.
               */
              .setContentCompressionEnabled(false)
              .build());
        addHeadersToRequest(head, INITIAL_REQUEST);
        return head;
    }

    private void checkChecksums(Set<Checksum> remoteChecksums,
          Optional<Set<Checksum>> knownChecksums)
          throws ThirdPartyTransferFailedCacheException {
        Map<ChecksumType, Checksum> checksums = uniqueIndex(remoteChecksums, Checksum::getType);

        boolean verified = false;
        if (knownChecksums.isPresent()) {
            for (Checksum ourChecksum : knownChecksums.get()) {
                ChecksumType type = ourChecksum.getType();

                if (checksums.containsKey(type)) {
                    checkChecksumEqual(ourChecksum, checksums.get(type));
                    verified = true;
                }
            }
        }

        if (_info.isVerificationRequired() && !verified) {
            String describe = remoteChecksums.stream()
                  .map(Object::toString)
                  .collect(Collectors.joining(","));
            throw new ThirdPartyTransferFailedCacheException(
                  "no useful checksum in HEAD response: {" + describe + "}");
        }
    }

    private static String headerValue(HttpResponse response, String headerName) {
        Header header = response.getFirstHeader(headerName);
        return header != null ? header.getValue() : null;
    }

    private static OptionalLong contentLength(HttpResponse response)
          throws ThirdPartyTransferFailedCacheException {
        Header header = response.getLastHeader("Content-Length");

        if (header == null) {
            return OptionalLong.empty();
        }

        try {
            return OptionalLong.of(Long.parseLong(header.getValue()));
        } catch (NumberFormatException e) {
            throw new ThirdPartyTransferFailedCacheException(
                  "server sent malformed Content-Length header", e);
        }
    }

    private static void checkChecksumEqual(Checksum expected, Checksum actual)
          throws ThirdPartyTransferFailedCacheException {
        if (expected.getType() != actual.getType()) {
            throw new RuntimeException("internal error: checksum comparison " +
                  "between different types (" + expected.getType() + " != " +
                  actual.getType());
        }

        if (!expected.equals(actual)) {
            throw new ThirdPartyTransferFailedCacheException(expected.getType().getName() + " " +
                  actual.getValue() + " != " + expected.getValue());
        }
    }

    private void deleteRemoteFile(String why) throws ThirdPartyTransferFailedCacheException {
        HttpDelete delete = buildDeleteRequest();

        try (CloseableHttpResponse response = _client.execute(delete)) {
            StatusLine status = response.getStatusLine();

            if (status.getStatusCode() >= 300) {
                throw new ThirdPartyTransferFailedCacheException("rejected DELETE: "
                      + status.getStatusCode() + " " + status.getReasonPhrase());
            }
        } catch (CacheException e) {
            throw new ThirdPartyTransferFailedCacheException("delete of " +
                  "remote file (triggered by " + why + ") failed: " + e.getMessage());
        } catch (IOException e) {
            throw new ThirdPartyTransferFailedCacheException("delete of " +
                  "remote file (triggered by " + why + ") failed: " + e.toString());
        }
    }

    private HttpDelete buildDeleteRequest() {
        HttpDelete delete = new HttpDelete(_info.getUri());
        delete.setConfig(RequestConfig.custom()
              .setConnectTimeout(CONNECTION_TIMEOUT)
              .setSocketTimeout(SOCKET_TIMEOUT)
              .build());
        addHeadersToRequest(delete, INITIAL_REQUEST);

        return delete;
    }

    private void addHeadersToRequest(HttpRequest request, Set<HeaderFlags> flags) {
        boolean dropAuthorizationHeader = flags.contains(HeaderFlags.NO_AUTHORIZATION_HEADER);

        _info.getHeaders().forEach(request::addHeader);

        // Propagate the SciTag to the remote party, unless the client already
        // supplied one explicitly via a TransferHeaderSciTag header.
        String transferTag = _info.getTransferTag();
        if (transferTag != null && !transferTag.isEmpty()
              && !request.containsHeader(SCITAG_HEADER)) {
            request.addHeader(SCITAG_HEADER, transferTag);
        }

        if (_info.hasTokenCredential() && !dropAuthorizationHeader) {
            request.addHeader("Authorization",
                  AUTH_BEARER +
                        new OpenIdCredentialRefreshable(_info.getTokenCredential(),
                              _client).getBearerToken());
        }

        if (dropAuthorizationHeader) {
            request.removeHeaders("Authorization");
        }
    }
}
