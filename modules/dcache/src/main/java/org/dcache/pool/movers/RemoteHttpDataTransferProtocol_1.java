package org.dcache.pool.movers;

import com.google.common.base.Optional;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.ThirdPartyTransferFailedCacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.pool.movers.MoverChannel.AllocatorMode;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Checksums;
import org.dcache.util.Version;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.collect.Maps.uniqueIndex;
import static org.dcache.namespace.FileAttribute.CHECKSUM;

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
public class RemoteHttpDataTransferProtocol_1 implements MoverProtocol,
        ChecksumMover
{
    private final static Logger _log =
        LoggerFactory.getLogger(RemoteHttpDataTransferProtocol_1.class);

    /** Maximum time to wait when establishing a connection. */
    private static final int CONNECTION_TIMEOUT = (int) TimeUnit.MINUTES.toMillis(1);

    /** Maximum time to wait for next packet from remote server. */
    private static final int SOCKET_TIMEOUT = (int) TimeUnit.MINUTES.toMillis(1);

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
    private static final double POST_PROCESSING_BANDWIDTH = 100 * 1024 * 1024 / 1_000.0;

    /** Number of milliseconds between successive requests. */
    private static final long DELAY_BETWEEN_REQUESTS = 5_000;

    /**
     * Maximum number of redirections to follow.
     * Note that, although RFC 2068 section 10.3 recommends a maximum of 5,
     * both firefox and webkit currently limit (by default) to 20 redirections.
     */
    private static final int MAX_REDIRECTIONS = 20;

    // REVISIT: we may wish to generate a value based on the algorithms dCache
    // supports
    private static final String WANT_DIGEST_VALUE = "adler32;q=1, md5;q=0.8";

    protected static final String USER_AGENT = "dCache/" +
            Version.of(RemoteHttpDataTransferProtocol_1.class).getVersion();

    // Pool-supplied factory for on-transfer checksums, null if disabled.
    private ChecksumFactory _onTransfer;

    // The RepositoryChannel to satisfy on-transfer checksum calculation.
    private ChecksumChannel _onTransferChecksumChannel;

    // The RepositoryChannel to verify data integrety when remote supplied
    // checksums that don't overlap with the on-transfer checksum.
    private ChecksumChannel _remoteSuppliedChecksumChannel;

    private volatile MoverChannel<RemoteHttpDataTransferProtocolInfo> _channel;
    private Checksum _remoteSuppliedChecksum;

    private CloseableHttpClient _client;

    public RemoteHttpDataTransferProtocol_1(CellEndpoint cell)
    {
        // constructor needed by Pool mover contract.
    }

    private static void checkThat(boolean isOk, String message) throws CacheException
    {
        if (!isOk) {
            throw new CacheException(message);
        }
    }

    @Override
    public void runIO(FileAttributes attributes, RepositoryChannel channel,
            ProtocolInfo genericInfo, Allocator allocator, IoMode access)
            throws CacheException, IOException, InterruptedException
    {
        _log.debug("info={}, attributes={},  access={}", genericInfo,
                attributes, access);
        RemoteHttpDataTransferProtocolInfo info =
                (RemoteHttpDataTransferProtocolInfo) genericInfo;
        _channel = new MoverChannel<>(access, attributes, info, channel,
                allocator, AllocatorMode.HARD);

        _client = createHttpClient();
        try {
            switch (access) {
            case WRITE:
                receiveFile(info);
                break;

            case READ:
                checkThat(!info.isVerificationRequired() || attributes.isDefined(CHECKSUM),
                        "checksum verification failed: file has no checksum");
                sendAndCheckFile(info);
                break;
            }
        } finally {
            _client.close();
        }
    }

    protected CloseableHttpClient createHttpClient() throws CacheException
    {
        return HttpClients.custom().setUserAgent(USER_AGENT).build();
    }

    private void receiveFile(final RemoteHttpDataTransferProtocolInfo info)
            throws ThirdPartyTransferFailedCacheException
    {
        HttpGet get = new HttpGet(info.getUri());
        get.setProtocolVersion(HttpVersion.HTTP_1_1);
        get.addHeader("Want-Digest", WANT_DIGEST_VALUE);
        for (Map.Entry<String,String> header : info.getHeaders().entrySet()) {
            get.addHeader(header.getKey(), header.getValue());
        }
        get.setConfig(RequestConfig.custom().
                setConnectTimeout(CONNECTION_TIMEOUT).
                setSocketTimeout(SOCKET_TIMEOUT).
                build());

        try (CloseableHttpResponse response = _client.execute(get)) {
            StatusLine statusLine = response.getStatusLine();
            if (statusLine.getStatusCode() >= 300) {
                throw new ThirdPartyTransferFailedCacheException("remote " +
                        "server rejected GET: " + statusLine.getStatusCode() +
                        " " + statusLine.getReasonPhrase());
            }

            String rfc3230 = headerValue(response, "Digest");
            Map<ChecksumType,Checksum> checksums =
                    uniqueIndex(Checksums.decodeRfc3230(rfc3230), Checksum::getType);

            if (!checksums.isEmpty()) {
                if (_onTransfer != null && checksums.containsKey(_onTransfer.getType())) {
                    _remoteSuppliedChecksum = checksums.get(_onTransfer.getType());
                } else {
                    _remoteSuppliedChecksum = Checksums.preferrredOrder().min(checksums.values());
                }
            }

            if (_remoteSuppliedChecksum == null && info.isVerificationRequired()) {
                throw new ClientProtocolException("failed to verify transfer: " +
                                                  "server sent no useful checksum: " +
                                                  (rfc3230 == null ? "(none sent)" : rfc3230));
            }

            // NB. we MUST NOT close RepositoryChannel as pool wants to do this
            RepositoryChannel to = decorateForChecksumCalculation(_channel);

            HttpEntity entity = response.getEntity();
            if (entity == null) {
                throw new ClientProtocolException("Response contains no content");
            }

            entity.writeTo(Channels.newOutputStream(to));
        } catch (IOException e) {
            throw new ThirdPartyTransferFailedCacheException(e.toString(), e);
        }

        if (_remoteSuppliedChecksum != null) {
            Checksum transferChecksum  = (_remoteSuppliedChecksumChannel != null) ?
                _remoteSuppliedChecksumChannel.getChecksum() : _onTransferChecksumChannel.getChecksum();

            if (!_remoteSuppliedChecksum.equals(transferChecksum)) {
                throw new ThirdPartyTransferFailedCacheException(
                        String.format("Received data does not match remote server's checksum (%s != %s)",
                        transferChecksum, _remoteSuppliedChecksum));
            }
        }
    }

    private RepositoryChannel decorateForChecksumCalculation(RepositoryChannel
            baseChannel)
    {
        RepositoryChannel channel = baseChannel;

        if (_onTransfer != null) {
            channel = _onTransferChecksumChannel = new ChecksumChannel(channel, _onTransfer);
        }

        if (_remoteSuppliedChecksum != null &&
                (_onTransfer == null || _onTransfer.getType() != _remoteSuppliedChecksum.getType())) {
            try {
                channel = _remoteSuppliedChecksumChannel = new ChecksumChannel(channel,
                        ChecksumFactory.getFactoryFor(_remoteSuppliedChecksum));
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("cannot find algorithm: " +
                        e.getMessage(), e);
            }
        }

        return channel;
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

        for (int attempt = 0; attempt < MAX_REDIRECTIONS; attempt++) {
            HttpPut put = buildPutRequest(location, info.getHeaders(), length);

            try (CloseableHttpResponse response = _client.execute(put)) {
                StatusLine status = response.getStatusLine();
                switch (status.getStatusCode()) {
                case 200: /* OK (not actually a valid response from PUT) */
                case 201: /* Created */
                    return;

                case 300: /* Multiple Choice */
                case 301: /* Moved Permanently */
                case 302: /* Found (REVISIT: should we treat this as an error?) */
                case 307: /* Temporary Redirect */
                case 308: /* Permanent Redirect */
                    String locationHeader = response.getFirstHeader("Location").getValue();
                    if (locationHeader == null) {
                        throw new ThirdPartyTransferFailedCacheException("remote " +
                                "server replied " + status.getStatusCode() +
                                " (" + status.getReasonPhrase() + ") without a " +
                                "Location header");
                    }


                    try {
                        location = URI.create(locationHeader);
                    } catch (IllegalArgumentException e) {
                        throw new ThirdPartyTransferFailedCacheException("remote " +
                                "server redirected to invalid URL '" +
                                locationHeader + "': " + e.getMessage());
                    }
                    break;

                /* Treat all other responses as a failure. */
                default:
                    throw new ThirdPartyTransferFailedCacheException("remote " +
                            "server rejected PUT: " + status.getStatusCode() +
                            " " + status.getReasonPhrase());
                }
            } catch (IOException e) {
                _log.error("problem connecting: {}", e.toString());
                throw new ThirdPartyTransferFailedCacheException("failed to " +
                        "connect to server: " + e.toString(), e);
            }
        }

        _log.error("too many redirects, last location was: {}", location);
        throw new ThirdPartyTransferFailedCacheException("exceeded maximum " +
                "number of redirections; last location was " + location);
    }

    private HttpPut buildPutRequest(URI location, Map<String,String> extraHeaders, long length)
    {
        HttpPut request = new HttpPut(location);
        request.setProtocolVersion(HttpVersion.HTTP_1_1);
        request.setConfig(RequestConfig.custom().
                setConnectTimeout(CONNECTION_TIMEOUT).
                setExpectContinueEnabled(true).
                setSocketTimeout(0).
                build());
        for (Map.Entry<String,String> header : extraHeaders.entrySet()) {
            request.addHeader(header.getKey(), header.getValue());
        }

        request.setEntity(new InputStreamEntity(Channels.newInputStream(_channel), length));

        // FIXME add SO_KEEPALIVE setting

        return request;
    }

    private void verifyRemoteFile(RemoteHttpDataTransferProtocolInfo info)
            throws ThirdPartyTransferFailedCacheException
    {
        FileAttributes attributes = _channel.getFileAttributes();
        boolean isFirstAttempt = true;

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
        long t_max = POST_PROCESSING_OFFSET +
                (long)(attributes.getSize() / POST_PROCESSING_BANDWIDTH);
        long deadline = System.currentTimeMillis() + t_max;

        try {
            while (System.currentTimeMillis() < deadline) {
                long sleepFor = Math.min(deadline - System.currentTimeMillis(),
                        DELAY_BETWEEN_REQUESTS);
                if (!isFirstAttempt && sleepFor > 0) {
                    Thread.sleep(sleepFor);
                }
                isFirstAttempt = false;

                HttpHead head = buildHeadRequest(info.getUri(), info.getHeaders());
                try (CloseableHttpResponse response = _client.execute(head)) {
                    StatusLine status = response.getStatusLine();

                    if (status.getStatusCode() >= 300) {
                        throw new ThirdPartyTransferFailedCacheException("remote " +
                                "server rejected HEAD: " + status.getStatusCode() +
                                " " + status.getReasonPhrase());
                    }

                    Long length = getContentLength(response);

                    if (length == null || (attributes.getSize() != 0 && length == 0)) {
                        continue;
                    }

                    if (attributes.getSize() != length) {
                        throw new ThirdPartyTransferFailedCacheException(
                                String.format("server reported wrong file size (%d != %d)",
                                length, attributes.getSize()));
                    }

                    String rfc3230 = headerValue(response, "Digest");
                    checkChecksums(info, rfc3230, attributes.getChecksumsIfPresent());
                    return;
                } catch (IOException e) {
                    throw new ThirdPartyTransferFailedCacheException("failed to " +
                            "connect to server: " + e.toString(), e);
                }
            }
        } catch (InterruptedException e) {
            throw new ThirdPartyTransferFailedCacheException("pool is shutting down", e);
        }

        throw new ThirdPartyTransferFailedCacheException("remote server failed " +
                "to provide length after " + (t_max / 1_000) + "s");
    }

    private HttpHead buildHeadRequest(URI location, Map<String,String> extraHeaders)
    {
        HttpHead request = new HttpHead(location);
        request.addHeader("Want-Digest", WANT_DIGEST_VALUE);
        request.setProtocolVersion(HttpVersion.HTTP_1_1);
        request.setConfig(RequestConfig.custom().
                setConnectTimeout(CONNECTION_TIMEOUT).
                setSocketTimeout(SOCKET_TIMEOUT).
                build());
        for (Map.Entry<String,String> header : extraHeaders.entrySet()) {
            request.addHeader(header.getKey(), header.getValue());
        }

        return request;
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
            throw new ThirdPartyTransferFailedCacheException("server sent no useful checksum: " +
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
        HttpDelete delete = new HttpDelete(info.getUri());
        delete.setProtocolVersion(HttpVersion.HTTP_1_1);
        delete.setConfig(RequestConfig.custom().
                setConnectTimeout(CONNECTION_TIMEOUT).
                setSocketTimeout(SOCKET_TIMEOUT).
                build());
        for (Map.Entry<String,String> header : info.getHeaders().entrySet()) {
            delete.addHeader(header.getKey(), header.getValue());
        }

        try (CloseableHttpResponse response = _client.execute(delete)) {
            StatusLine status = response.getStatusLine();

            if (status.getStatusCode() >= 300) {
                throw new ThirdPartyTransferFailedCacheException("remote " +
                        "server rejected DELETE: " + status.getStatusCode() +
                        " " + status.getReasonPhrase());
            }
        } catch (CacheException e) {
            throw new ThirdPartyTransferFailedCacheException("delete of " +
                    "remote file (triggered by " + why + ") failed: " + e.getMessage());
        } catch (IOException e) {
            throw new ThirdPartyTransferFailedCacheException("delete of " +
                    "remote file (triggered by " + why + ") failed: " + e.toString());
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

    @Override
    public void enableTransferChecksum(ChecksumType suggestedAlgorithm)
            throws NoSuchAlgorithmException
    {
        _onTransfer = ChecksumFactory.getFactory(suggestedAlgorithm);
    }

    @Override
    public Checksum getActualChecksum()
    {
        return _onTransferChecksumChannel != null ? _onTransferChecksumChannel.getChecksum() : null;
    }

    @Override
    public Checksum getExpectedChecksum()
    {
        return _remoteSuppliedChecksum;
    }
}

