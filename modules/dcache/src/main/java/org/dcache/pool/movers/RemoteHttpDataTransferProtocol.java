package org.dcache.pool.movers;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolException;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ThirdPartyTransferFailedCacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.auth.OpenIdCredentialRefreshable;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Checksums;
import org.dcache.util.Version;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.collect.Maps.uniqueIndex;
import static diskCacheV111.util.ThirdPartyTransferFailedCacheException.checkThirdPartyTransferSuccessful;
import static dmg.util.Exceptions.getMessageWithCauses;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.util.ByteUnit.GiB;
import static org.dcache.util.ByteUnit.MiB;
import static org.dcache.util.Exceptions.genericCheck;
import static org.dcache.util.Exceptions.messageOrClassName;
import static org.dcache.util.Strings.describeSize;
import static org.dcache.util.Strings.toThreeSigFig;
import static org.dcache.util.TimeUtils.describeDuration;

/**
 * This class implements transfers of data between a pool and some remote
 * HTTP server.  Both writing data into dCache and reading from dCache are
 * supported (HTTP GET and PUT respectively).  On-transfer checksum calculation
 * is supported.  Support is also included for RFC-3230, which allows the remote
 * server to specify one or more checksums as part of a response to a GET or
 * HEAD request.
 *
 * If the remote server supports RFC-3230 then this is used to discover the
 * remote file's checksum(s).  Using this, dCache will always try to verify the
 * transfer was successful.  dCache will be unable to do this only if the file is
 * sent to a remote server that provides a set of checksums that doesn't overlap
 * with the set of checksums dCache knows for this file.
 *
 * The mover supports a require-checksum-verification flag.
 *
 * When enabled, the integrity of the transferred data must be verified by
 * checking a remote supplied checksum matches one known locally (either
 * calculated as part of the transfer or already know for this file).  If the
 * flag is enabled and verification is impossible (e.g., the remote server
 * supplied no checksums) then the transfer will fail.
 *
 * If require-checksum-verification flag is disabled then a transfer will not
 * fail if the remote server supplies no checksum; however, if checksums are
 * supplied then they are checked against locally known checksums (either
 * calculated as part of the transfer or already known for this file) and a
 * mismatch will fail the transfer.
 *
 *
 * WRITE REQUESTS
 *
 * The pool accepts only a single client-supplied (i.e., from the remote server)
 * checksum value.  Therefore if the remote server supplies more than one
 * checksum then one is selected: either matching the pool's on-transfer
 * checksum choice or a hard-coded preference list.
 *
 * If require-checksum-verification is enabled and the remote server supplied
 * no checksum that dCache understands, or the server doesn't support RFC-3230,
 * then the transfer fails.
 *
 *
 * READ REQUESTS
 *
 * When the request is for reading data (an HTTP PUT request) then the mover
 * will copy the file to the remote server and try to verify that the file
 * arrived OK using the HTTP HEAD command.  If the HEAD request fails or the
 * Content-Length value is wrong, or (if the remote server supports RFC-3230)
 * the supplied checksums indicate data corruption then the mover will fail
 * the transfer.
 *
 * If checksum-verification-required is enabled and the remote server does not
 * support RFC-3230 or none of the checksums provided by the remote server
 * were calculated using the same algorithm as a known checksum for this file
 * then the mover will fail the transfer.
 *
 * If checksum-verification-required is disabled then a lack of checksum
 * verification does not fail the transfer.
 *
 * If the PUT request fails, for whatever reason, then the mover will attempt
 * to clear up the transfer by deleting the remote copy via the HTTP DELETE
 * command.  If the cleanup is successful then the error triggering the cleanup
 * is reported.  If the cleanup is unsuccessful then an error is reported
 * containing both the error in removing the remote file and the error that
 * triggered the delete.
 */
public class RemoteHttpDataTransferProtocol implements MoverProtocol,
        ChecksumMover
{
    private enum HeaderFlags {
        /** Do not include any Authorization request header. */
        NO_AUTHORIZATION_HEADER
    }

    private static final Set<HeaderFlags> REDIRECTED_REQUEST
            = EnumSet.of(HeaderFlags.NO_AUTHORIZATION_HEADER);

    private static final Set<HeaderFlags> INITIAL_REQUEST
            = EnumSet.noneOf(HeaderFlags.class);

    private static final Logger _log =
        LoggerFactory.getLogger(RemoteHttpDataTransferProtocol.class);

    /** Maximum time to wait when establishing a connection. */
    private static final int CONNECTION_TIMEOUT = (int) TimeUnit.MINUTES.toMillis(1);

    /** Maximum time to wait for next packet from remote server. */
    private static final int SOCKET_TIMEOUT = (int) TimeUnit.MINUTES.toMillis(1);

    /**
     * Maximum time to wait for next packet from remote server for GET requests.
     * This needs to be longer as DPM can block on GET requests while calculating
     * the checksum.
     */
    private static final int GET_SOCKET_TIMEOUT = (int) TimeUnit.MINUTES.toMillis(20);

    /**
     * Expected maximum delay all post-processing files will experience,
     * in milliseconds.
     */
    private static final long POST_PROCESSING_OFFSET = 60_000;

    /**
     * Expected minimum (effective) internal IO bandwidth of the remote
     * server, in bytes per millisecond.  This is used to estimate how long
     * any file post-processing (like checksum calculation) will take.
     */
    private static final double POST_PROCESSING_BANDWIDTH = MiB.toBytes(100) / 1_000.0;

    /** Number of milliseconds between successive requests. */
    private static final long DELAY_BETWEEN_REQUESTS = 5_000;

    /**
     * A guess on how long to retry a GET request.  Since the file size is
     * unknown, this value may be insufficient if the file is larger.
     */
    private static final long GET_RETRY_DURATION = maxRetryDuration(GiB.toBytes(2L));
    private static final String GET_RETRY_DURATION_DESCRIPTION = describeDuration(GET_RETRY_DURATION, MILLISECONDS);

    /**
     * Maximum number of redirections to follow.
     * Note that, although RFC 2068 section 10.3 recommends a maximum of 5,
     * both firefox and webkit currently limit (by default) to 20 redirections.
     */
    private static final int MAX_REDIRECTIONS = 20;

    private static final String AUTH_BEARER = "Bearer ";

    // REVISIT: we may wish to generate a value based on the algorithms dCache
    // supports
    private static final String WANT_DIGEST_VALUE = "adler32;q=1, md5;q=0.8";

    private static final RedirectStrategy DROP_AUTHORIZATION_HEADER = new DefaultRedirectStrategy() {

                @Override
                public HttpUriRequest getRedirect(final HttpRequest request,
                        final HttpResponse response, final HttpContext context)
                        throws ProtocolException
                {
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

    protected static final String USER_AGENT = "dCache/" +
            Version.of(RemoteHttpDataTransferProtocol.class).getVersion();

    private volatile MoverChannel<RemoteHttpDataTransferProtocolInfo> _channel;
    private Consumer<Checksum> _integrityChecker;

    private CloseableHttpClient _client;

    public RemoteHttpDataTransferProtocol(CellEndpoint cell)
    {
        // constructor needed by Pool mover contract.
    }

    private static void checkThat(boolean isOk, String message) throws CacheException
    {
        genericCheck(isOk, CacheException::new, message);
    }

    @Override
    public void acceptIntegrityChecker(Consumer<Checksum> integrityChecker)
    {
        _integrityChecker = integrityChecker;
    }

    @Override
    public void runIO(FileAttributes attributes, RepositoryChannel channel,
            ProtocolInfo genericInfo, Set<? extends OpenOption> access)
            throws CacheException, IOException, InterruptedException
    {
        _log.debug("info={}, attributes={},  access={}", genericInfo,
                attributes, access);
        RemoteHttpDataTransferProtocolInfo info =
                (RemoteHttpDataTransferProtocolInfo) genericInfo;
        _channel = new MoverChannel<>(access, attributes, info, channel);

        channel.optionallyAs(ChecksumChannel.class).ifPresent(c -> {
                    info.getDesiredChecksum().ifPresent(t -> {
                                try {
                                    c.addType(t);
                                } catch (IOException e) {
                                    _log.warn("Unable to calculate checksum {}: {}",
                                            t, messageOrClassName(e));
                                }
                            });
                });

        _client = createHttpClient();
        try {
            if (access.contains(StandardOpenOption.WRITE)) {
                receiveFile(info);
            } else {
                checkThat(!info.isVerificationRequired() || attributes.isDefined(CHECKSUM),
                        "checksum verification failed: file has no checksum");
                sendAndCheckFile(info);
            }
        } finally {
            _client.close();
        }
    }

    protected CloseableHttpClient createHttpClient() throws CacheException
    {
        return customise(HttpClients.custom()).build();
    }

    protected HttpClientBuilder customise(HttpClientBuilder builder) throws CacheException
    {
        return builder
                .setUserAgent(USER_AGENT)
                .setRedirectStrategy(DROP_AUTHORIZATION_HEADER);
    }

    private void receiveFile(final RemoteHttpDataTransferProtocolInfo info)
            throws ThirdPartyTransferFailedCacheException
    {
        HttpClientContext context = new HttpClientContext();
        try {
            try (CloseableHttpResponse response = doGet(info, context)) {
                String rfc3230 = headerValue(response, "Digest");
                Set<Checksum> checksums = Checksums.decodeRfc3230(rfc3230);
                checksums.forEach(_integrityChecker);

                if (checksums.isEmpty() && info.isVerificationRequired()) {
                    throw new ThirdPartyTransferFailedCacheException("no useful checksum in GET response: " +
                                                      (rfc3230 == null ? "(none sent)" : rfc3230));
                }

                HttpEntity entity = response.getEntity();
                if (entity == null) {
                    throw new ThirdPartyTransferFailedCacheException("GET response contains no content");
                }

                long length = entity.getContentLength();
                if (length > 0) {
                    _channel.truncate(length);
                }
                entity.writeTo(Channels.newOutputStream(_channel));
            } catch (SocketTimeoutException e) {
                String message = "socket timeout on GET (received "
                        + describeSize(_channel.getBytesTransferred()) + " of data; "
                        + describeSize(e.bytesTransferred) + " pending)";
                if (e.getMessage() != null) {
                    message += ": " + e.getMessage();
                }
                throw new ThirdPartyTransferFailedCacheException(message, e);
            } catch (IOException e) {
                throw new ThirdPartyTransferFailedCacheException(messageOrClassName(e), e);
            } catch (InterruptedException e) {
                throw new ThirdPartyTransferFailedCacheException("pool is shutting down", e);
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
    }

    private CloseableHttpResponse doGet(final RemoteHttpDataTransferProtocolInfo info,
            HttpContext context) throws IOException, ThirdPartyTransferFailedCacheException, InterruptedException
    {
        HttpGet get = new HttpGet(info.getUri());
        get.addHeader("Want-Digest", WANT_DIGEST_VALUE);
        addHeadersToRequest(info, get, INITIAL_REQUEST);
        get.setConfig(RequestConfig.custom()
                              .setConnectTimeout(CONNECTION_TIMEOUT)
                              .setSocketTimeout(GET_SOCKET_TIMEOUT)
                              .build());

        CloseableHttpResponse response = _client.execute(get, context);

        boolean isSuccessful = false;
        try {
            long deadline = System.currentTimeMillis() + GET_RETRY_DURATION;
            while (shouldRetry(response) && System.currentTimeMillis() < deadline) {
                Thread.sleep(DELAY_BETWEEN_REQUESTS);

                response.close();
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


    private static boolean shouldRetry(HttpResponse response)
    {
        // DPM will return 202 for GET or HEAD with Want-Digest if it's still
        // calculating the checksum.
        return response.getStatusLine().getStatusCode() == HttpStatus.SC_ACCEPTED;
    }

    private void sendAndCheckFile(RemoteHttpDataTransferProtocolInfo info)
            throws ThirdPartyTransferFailedCacheException
    {
        sendFile(info, _channel.getFileAttributes().getSize());

        try {
            verifyRemoteFile(info);
        } catch (ThirdPartyTransferFailedCacheException e) {
            deleteRemoteFile(e.getMessage(), info);
            throw new ThirdPartyTransferFailedCacheException("verification " +
                    "failed: " + e.getMessage());
        }
    }

    private void sendFile(RemoteHttpDataTransferProtocolInfo info, long length)
            throws ThirdPartyTransferFailedCacheException
    {
        URI location = info.getUri();
        List<URI> redirections = null;

        try {
            for (int redirectionCount = 0; redirectionCount < MAX_REDIRECTIONS; redirectionCount++) {
                HttpPut put = buildPutRequest(info, location, length,
                        redirectionCount > 0 ? REDIRECTED_REQUEST : INITIAL_REQUEST);

                try (CloseableHttpResponse response = _client.execute(put)) {
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
                            throw new ThirdPartyTransferFailedCacheException("missing Location in PUT response "
                                    + status.getStatusCode() + " " + status.getReasonPhrase());
                        }


                        try {
                            location = URI.create(locationHeader);
                        } catch (IllegalArgumentException e) {
                            throw new ThirdPartyTransferFailedCacheException("invalid Location " +
                                    locationHeader + " in PUT response "
                                    + status.getStatusCode() + " " + status.getReasonPhrase()
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
                    if (_channel.getBytesTransferred() != 0) {
                        message.append("; after sending ").append(describeSize(_channel.getBytesTransferred()));
                        try {
                            String percent = toThreeSigFig(100 * _channel.getBytesTransferred() / (double)_channel.size(), 1000);
                            message.append(" (").append(percent).append("%)");
                        } catch (IOException io) {
                            _log.warn("failed to discover file size: {}", messageOrClassName(io));
                        }
                    }
                    throw new ThirdPartyTransferFailedCacheException(message.toString(), e);
                } catch (IOException e) {
                    throw new ThirdPartyTransferFailedCacheException("problem sending data: " + messageOrClassName(e), e);
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

    /**
     * Build a PUT request for this attempt to upload the file.
     * @param info the information from the door
     * @param location The URL to target
     * @param length The size of the file
     * @param flags Options that control the PUT request
     * @return A corresponding PUT request.
     */
    private HttpPut buildPutRequest(RemoteHttpDataTransferProtocolInfo info,
            URI location, long length, Set<HeaderFlags> flags)
    {
        HttpPut put = new HttpPut(location);
        put.setConfig(RequestConfig.custom()
                                  .setConnectTimeout(CONNECTION_TIMEOUT)
                                  .setExpectContinueEnabled(true)
                                  .setSocketTimeout(0)
                                  .build());
        addHeadersToRequest(info, put, flags);
        put.setEntity(new InputStreamEntity(Channels.newInputStream(_channel), length));

        // FIXME add SO_KEEPALIVE setting

        return put;
    }

    /**
     * How long to retry a GET or HEAD request for a file with given size.
     * @param fileSize The file's size, in bytes.
     * @return the maximum retry duration, in milliseconds.
     */
    private static long maxRetryDuration(long fileSize)
    {
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
        return POST_PROCESSING_OFFSET + (long)(fileSize / POST_PROCESSING_BANDWIDTH);
    }

    private void verifyRemoteFile(RemoteHttpDataTransferProtocolInfo info)
            throws ThirdPartyTransferFailedCacheException
    {
        FileAttributes attributes = _channel.getFileAttributes();
        boolean isFirstAttempt = true;

        long t_max = maxRetryDuration(attributes.getSize());
        long deadline = System.currentTimeMillis() + t_max;

        try {
            while (System.currentTimeMillis() < deadline) {
                long sleepFor = Math.min(deadline - System.currentTimeMillis(),
                        DELAY_BETWEEN_REQUESTS);
                if (!isFirstAttempt && sleepFor > 0) {
                    Thread.sleep(sleepFor);
                }
                isFirstAttempt = false;

                HttpClientContext context = new HttpClientContext();
                HttpHead head = buildHeadRequest(info);
                try {
                    try (CloseableHttpResponse response = _client.execute(head, context)) {
                        StatusLine status = response.getStatusLine();

                        if (status.getStatusCode() >= 300) {
                            checkThirdPartyTransferSuccessful(!info.isVerificationRequired(),
                                    "rejected HEAD: %d %s", status.getStatusCode(),
                                    status.getReasonPhrase());
                            return;
                        }

                        if (shouldRetry(response)) {
                            continue;
                        }

                        Long length = getContentLength(response);

                        // REVISIT This is to support pre-2.12 dCache, which could
                        // give a '201 Created' response to a PUT request before
                        // post-processing (including checksum calculation) was
                        // completed and the final details registered in the
                        // namespace.  This problem is fixed with dCache v2.12 or
                        // later.
                        if (length == null || (attributes.getSize() != 0 && length == 0)) {
                            continue;
                        }

                        if (attributes.getSize() != length) {
                            throw new ThirdPartyTransferFailedCacheException(
                                    String.format("wrong Content-Length in HEAD response (%d != %d)",
                                    length, attributes.getSize()));
                        }

                        String rfc3230 = headerValue(response, "Digest");
                        checkChecksums(info, rfc3230, attributes.getChecksumsIfPresent());
                        return;
                    } catch (IOException e) {
                        throw new ThirdPartyTransferFailedCacheException("failed to " +
                                "connect to server: " + e.toString(), e);
                    }
                } catch (ThirdPartyTransferFailedCacheException e) {
                    List<URI> redirections = context.getRedirectLocations();
                    if (redirections != null && !redirections.isEmpty()) {
                        throw new ThirdPartyTransferFailedCacheException(e.getMessage() + "; redirections " + redirections, e.getCause());
                    } else {
                        throw e;
                    }

                }
            }
        } catch (InterruptedException e) {
            throw new ThirdPartyTransferFailedCacheException("pool is shutting down", e);
        }

        throw new ThirdPartyTransferFailedCacheException("remote server failed " +
                "to provide length after " + describeDuration(GET_RETRY_DURATION, MILLISECONDS));
    }

    private HttpHead buildHeadRequest(RemoteHttpDataTransferProtocolInfo info)
    {
        HttpHead head = new HttpHead(info.getUri());

        _channel.getFileAttributes().getChecksumsIfPresent()
                .map(v -> Checksums.asWantDigest(v))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .ifPresent(v -> head.addHeader("Want-Digest", v));

        head.setConfig(RequestConfig.custom()
                                  .setConnectTimeout(CONNECTION_TIMEOUT)
                                  .setSocketTimeout(SOCKET_TIMEOUT)
                                  .build());
        addHeadersToRequest(info, head, INITIAL_REQUEST);
        return head;
    }

    private void checkChecksums(RemoteHttpDataTransferProtocolInfo info,
            String rfc3230, Optional<Set<Checksum>> knownChecksums)
            throws ThirdPartyTransferFailedCacheException
    {
        Map<ChecksumType,Checksum> checksums =
                uniqueIndex(Checksums.decodeRfc3230(rfc3230), Checksum::getType);

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

        if (info.isVerificationRequired() && !verified) {
            throw new ThirdPartyTransferFailedCacheException("no useful checksum in HEAD response: " +
                                                             (rfc3230 == null ? "(none sent)" : rfc3230));
        }
    }

    private static String headerValue(HttpResponse response, String headerName)
    {
        Header header = response.getFirstHeader(headerName);
        return header != null ? header.getValue() : null;
    }

    private static Long getContentLength(HttpResponse response)
            throws ThirdPartyTransferFailedCacheException
    {
        Header header = response.getLastHeader("Content-Length");

        if (header == null) {
            return null;
        }

        try {
            return Long.parseLong(header.getValue());
        } catch (NumberFormatException e) {
            throw new ThirdPartyTransferFailedCacheException("server sent malformed Content-Length header", e);
        }
    }

    private static void checkChecksumEqual(Checksum expected, Checksum actual)
            throws ThirdPartyTransferFailedCacheException
    {
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

    private void deleteRemoteFile(String why, RemoteHttpDataTransferProtocolInfo info)
            throws ThirdPartyTransferFailedCacheException
    {
        HttpDelete delete = buildDeleteRequest(info);

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

    private HttpDelete buildDeleteRequest(RemoteHttpDataTransferProtocolInfo info) {
        HttpDelete delete = new HttpDelete(info.getUri());
        delete.setConfig(RequestConfig.custom()
                                 .setConnectTimeout(CONNECTION_TIMEOUT)
                                 .setSocketTimeout(SOCKET_TIMEOUT)
                                 .build());
        addHeadersToRequest(info, delete, INITIAL_REQUEST);

        return delete;
    }

    private void addHeadersToRequest(RemoteHttpDataTransferProtocolInfo info,
                                    HttpRequest request,
                                    Set<HeaderFlags> flags)
    {
        boolean dropAuthorizationHeader = flags.contains(HeaderFlags.NO_AUTHORIZATION_HEADER);

        info.getHeaders().forEach(request::addHeader);

        if (info.hasTokenCredential() && !dropAuthorizationHeader) {
            request.addHeader("Authorization",
                    AUTH_BEARER +
                            new OpenIdCredentialRefreshable(info.getTokenCredential(), _client).getBearerToken());
        }

        if (dropAuthorizationHeader) {
            request.removeHeaders("Authorization");
        }
    }

    @Override
    public long getLastTransferred()
    {
        MoverChannel<RemoteHttpDataTransferProtocolInfo> channel = _channel;
        return channel == null ? System.currentTimeMillis() : channel.getLastTransferred();
    }

    @Override
    public long getBytesTransferred()
    {
        MoverChannel<RemoteHttpDataTransferProtocolInfo> channel = _channel;
        return channel == null ? 0 : channel.getBytesTransferred();
    }

    @Override
    public long getTransferTime()
    {
        MoverChannel<RemoteHttpDataTransferProtocolInfo> channel = _channel;
        return channel == null ? 0 : channel.getTransferTime();
    }
}
