/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014-2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.webdav.transfer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import eu.emi.security.authn.x509.X509Credential;
import io.milton.http.Response;
import io.milton.http.Response.Status;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;
import org.apache.curator.shaded.com.google.common.base.Splitter;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.HttpConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.security.auth.Subject;
import javax.servlet.AsyncContext;
import javax.servlet.ServletResponseWrapper;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import diskCacheV111.services.TransferManagerHandler;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.MissingResourceCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpsDataTransferProtocolInfo;
import diskCacheV111.vehicles.transferManager.CancelTransferMessage;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;
import diskCacheV111.vehicles.transferManager.RemoteTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.TransferCompleteMessage;
import diskCacheV111.vehicles.transferManager.TransferFailedMessage;
import diskCacheV111.vehicles.transferManager.TransferStatusQueryMessage;

import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.OpenIdCredential;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.ChecksumType;
import org.dcache.util.Checksums;
import org.dcache.util.URIs;
import org.dcache.vehicles.FileAttributes;
import org.dcache.webdav.transfer.CopyFilter.CredentialSource;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.servlet.http.HttpServletResponse.SC_ACCEPTED;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.util.ByteUnit.MiB;
import static org.dcache.webdav.transfer.CopyFilter.CredentialSource.*;

/**
 * This class provides the basis for interactions with the remotetransfer
 * service.  It is used by the CopyFilter to manage a requested transfer and
 * to provide feedback on that transfer in the form of performance markers.
 * <p>
 * The performance markers are similar to those provided during an FTP
 * transfer.  They have the form:
 * <code>
 *     Perf Marker
 *      Timestamp: 1360578938
 *      Stripe Index: 0
 *      Stripe Bytes Transferred: 49397760
 *      Total Stripe Count: 2
 *     End
 * </code>
 *
 * Once the transfer has completed successfully, {@code success: Created} is
 * reported.  On failure {@code failure: <explanation>} is returned.
 * <p>
 * Although the third-party transfer protocol, described in CopyFilter is
 * documented as supporting 'https' URIs, this implementation supports
 * different transports for the third-party transfer.
 */
public class RemoteTransferHandler implements CellMessageReceiver
{

    /**
     * The different directions that the data will travel.
     */
    public enum Direction
    {
        /** Request to pull data from remote site. */
        PULL("Source"),

        /** Request to push data to some remote site. */
        PUSH("Destination");

        private final String header;

        Direction(String header)
        {
            this.header = header;
        }

        public String getHeaderName()
        {
            return header;
        }
    }


    /**
     * The different transport schemes supported.
     */
    public enum TransferType {
        GSIFTP("gsiftp", EnumSet.of(GRIDSITE)),
        HTTP(  "http",   EnumSet.of(NONE)),
        HTTPS( "https",  EnumSet.of(GRIDSITE, OIDC, NONE));

        private static final ImmutableMap<String,TransferType> BY_SCHEME =
            ImmutableMap.of("gsiftp", GSIFTP, "http", HTTP, "https", HTTPS);

        private final EnumSet<CredentialSource> _supported;
        private final String _scheme;

        TransferType(String scheme, EnumSet<CredentialSource> supportedSources)
        {
            _supported = EnumSet.copyOf(supportedSources);
            _scheme = scheme;
        }

        public boolean isSupported(CredentialSource source)
        {
            return _supported.contains(source);
        }

        public String getScheme()
        {
            return _scheme;
        }

        public static TransferType fromScheme(String scheme)
                throws ErrorResponseException
        {
            TransferType type = BY_SCHEME.get(scheme.toLowerCase());

            if (type == null) {
                throw new ErrorResponseException(Status.SC_BAD_REQUEST,
                        "URI contains unsupported scheme " + scheme
                                + ". Supported schemes are "
                                + Joiner.on(", ").join(validSchemes()));
            }

            return type;
        }

        public static Set<String> validSchemes()
        {
            return BY_SCHEME.keySet();
        }
    }

    public enum TransferFlag {
        REQUIRE_VERIFICATION
    }

    private static final Set<AccessMask> READ_ACCESS_MASK =
            EnumSet.of(AccessMask.READ_DATA);

    private static final Logger LOGGER =
            LoggerFactory.getLogger(RemoteTransferHandler.class);
    private static final long DUMMY_LONG = 0;

    private final Map<Long,RemoteTransfer> _transfers = new ConcurrentHashMap<>();

    private long _performanceMarkerPeriod;
    private CellStub _transferManager;
    private PnfsHandler _pnfs;
    private final ScheduledExecutorService _scheduler = Executors.newScheduledThreadPool(1);

    @Required
    public void setTransferManagerStub(CellStub stub)
    {
        _transferManager = stub;
    }

    @Required
    public void setPnfsStub(CellStub stub)
    {
        _pnfs = new PnfsHandler(stub);
    }

    @Required
    public void setPerformanceMarkerPeroid(long peroid)
    {
        _performanceMarkerPeriod = peroid;
    }

    public long getPerformanceMarkerPeroid()
    {
        return _performanceMarkerPeriod;
    }

    @PreDestroy
    public void stop()
    {
        _scheduler.shutdown();
    }

    /**
     * Start a transfer and block until that transfer is complete.
     * @return a description of the error, if there was a problem.
     */
    public ListenableFuture<Optional<String>> acceptRequest(
            ImmutableMap<String,String> transferHeaders,
            Subject subject, Restriction restriction, FsPath path, URI remote,
            Object credential, Direction direction, EnumSet<TransferFlag> flags,
            boolean overwriteAllowed, Optional<String> wantDigest)
            throws ErrorResponseException, InterruptedException
    {
        RemoteTransfer transfer = new RemoteTransfer(subject, restriction,
                path, remote, credential, flags, transferHeaders, direction,
                overwriteAllowed, wantDigest);

        return transfer.start();
    }

    public void messageArrived(TransferCompleteMessage message)
    {
        RemoteTransfer transfer = _transfers.get(message.getId());
        if (transfer != null) {
            transfer.completed(null);
        }
    }

    public void messageArrived(TransferFailedMessage message)
    {
        RemoteTransfer transfer = _transfers.get(message.getId());
        if (transfer != null) {
            transfer.completed(String.valueOf(message.getErrorObject()));
        }
    }

    /**
     * Class that represents a client's request to transfer a file to some
     * remote server.
     * <p>
     * This class needs to be aware of the client closing its end of the TCP
     * connection while the transfer underway.  In the protocol, this is used
     * to indicate that the transfer should be cancelled.  Unfortunately, there
     * is no container-independent mechanism for discovering this, so
     * Jetty-specific code is needed.
     */
    private class RemoteTransfer
    {
        private final TransferType _type;
        private final Subject _subject;
        private final Restriction _restriction;
        private final FsPath _path;
        private final URI _destination;
        @Nullable
        private final PrivateKey _privateKey;
        @Nullable
        private final X509Certificate[] _certificateChain;
        @Nullable
        private final OpenIdCredential _oidCredential;
        private final CredentialSource _source;
        private final EnumSet<TransferFlag> _flags;
        private final ImmutableMap<String,String> _transferHeaders;
        private final Direction _direction;
        private final boolean _overwriteAllowed;
        private final Optional<String> _wantDigest;
        private final PnfsHandler _pnfs;
        private final SettableFuture<Optional<String>> _transferResult = SettableFuture.create();
        private long _id;
        private final EndPoint _endpoint = HttpConnection.getCurrentConnection().getEndPoint();

        private PnfsId _pnfsId;
        private Optional<String> _digestValue;
        private boolean _transferReportedAsUnknown;
        private ScheduledFuture<?> _sendingMarkers;
        private AsyncContext _async;

        public RemoteTransfer(Subject subject, Restriction restriction,
                FsPath path, URI destination, @Nullable Object credential,
                EnumSet<TransferFlag> flags, ImmutableMap<String,String> transferHeaders,
                Direction direction, boolean overwriteAllowed, Optional<String> wantDigest)
                throws ErrorResponseException
        {
            _subject = subject;
            _restriction = restriction;
            _pnfs = new PnfsHandler(RemoteTransferHandler.this._pnfs, _subject, _restriction);
            _path = path;
            _destination = destination;
            _type = TransferType.fromScheme(destination.getScheme());
            if (credential instanceof X509Credential) {
                _privateKey = ((X509Credential)credential).getKey();
                _certificateChain = ((X509Credential)credential).getCertificateChain();
                _source = CredentialSource.GRIDSITE;
                _oidCredential = null;
            } else if (credential instanceof OpenIdCredential) {
                _privateKey = null;
                _certificateChain = null;
                _source = CredentialSource.OIDC;
                _oidCredential = (OpenIdCredential) credential;
            } else if (credential == null) {
                _privateKey = null;
                _certificateChain = null;
                _source = null;
                _oidCredential = null;
            } else {
                throw new IllegalArgumentException("Credential not supported for Third-Party Transfer");
            }

            _flags = flags;
            _transferHeaders = transferHeaders;
            _direction = direction;
            _overwriteAllowed = overwriteAllowed;
            _wantDigest = wantDigest;
        }


        /**
         * Obtain the PnfsId of the local file, creating it as necessary.
         */
        private FileAttributes resolvePath() throws ErrorResponseException
        {
            try {
                switch (_direction) {
                case PUSH:
                    EnumSet<FileAttribute> desired = _wantDigest.isPresent()
                            ? EnumSet.of(PNFSID, SIZE, TYPE, CHECKSUM)
                            : EnumSet.of(PNFSID, SIZE, TYPE);
                    try {
                        FileAttributes attributes = _pnfs.getFileAttributes(_path.toString(),
                                desired, READ_ACCESS_MASK, false);

                        if (attributes.getFileType() != FileType.REGULAR) {
                            throw new ErrorResponseException(Response.Status.SC_BAD_REQUEST, "Not a file");
                        }

                        if (!attributes.isDefined(SIZE)) {
                            throw new ErrorResponseException(Response.Status.SC_CONFLICT, "File upload in progress");
                        }

                        return attributes;
                    } catch (FileNotFoundCacheException e) {
                        throw new ErrorResponseException(Response.Status.SC_NOT_FOUND, "no such file");
                    }

                case PULL:
                    PnfsCreateEntryMessage msg;
                    FileAttributes attributes = FileAttributes.of()
                            .fileType(FileType.REGULAR)
                            .xattr("xdg.origin.url", _destination.toASCIIString())
                            .build();
                    try {
                        msg = _pnfs.createPnfsEntry(_path.toString(), attributes);
                    } catch (FileExistsCacheException e) {
                        /* REVISIT: This should be moved to PnfsManager with a
                         * flag in the PnfsCreateEntryMessage.
                         */
                        if (!_overwriteAllowed) {
                            throw e;
                        }
                        _pnfs.deletePnfsEntry(_path.toString(), EnumSet.of(FileType.REGULAR));
                        msg = _pnfs.createPnfsEntry(_path.toString(),  attributes);
                    }
                    return msg.getFileAttributes();

                default:
                    throw new ErrorResponseException(Response.Status.SC_INTERNAL_SERVER_ERROR,
                            "Unexpected direction: " + _direction);
                }
            } catch (PermissionDeniedCacheException e) {
                LOGGER.debug("Permission denied: {}", e.getMessage());
                throw new ErrorResponseException(Response.Status.SC_UNAUTHORIZED,
                        "Permission denied");
            } catch (CacheException e) {
                LOGGER.error("failed query file {} for copy request: {}", _path,
                        e.getMessage());
                throw new ErrorResponseException(Response.Status.SC_INTERNAL_SERVER_ERROR,
                        "Internal problem with server");
            }
        }


        public synchronized ListenableFuture<Optional<String>> start()
                throws ErrorResponseException, InterruptedException
        {
            checkState(_id == 0, "Start already called.");

            FileAttributes attributes = resolvePath();
            _pnfsId = attributes.getPnfsId();

            RemoteTransferManagerMessage message =
                    new RemoteTransferManagerMessage(_destination, _path,
                            _direction == Direction.PULL, DUMMY_LONG,
                            buildProtocolInfo());

            message.setSubject(_subject);
            message.setRestriction(_restriction);
            message.setPnfsId(_pnfsId);
            try {
                _id = _transferManager.sendAndWait(message).getId();
                _transfers.put(_id, this);
                addDigestResponseHeader(attributes);
            } catch (NoRouteToCellException | TimeoutCacheException e) {
                LOGGER.error("Failed to send request to transfer manager: {}", e.getMessage());
                throw new ErrorResponseException(Response.Status.SC_INTERNAL_SERVER_ERROR,
                        "transfer service unavailable");
            } catch (CacheException e) {
                LOGGER.error("Error from transfer manager: {}", e.getMessage());
                throw new ErrorResponseException(Response.Status.SC_INTERNAL_SERVER_ERROR,
                        "transfer not accepted: " + e.getMessage());
            }

            HttpServletResponse servletResponse = ServletResponse.getResponse();
            servletResponse.setStatus(SC_ACCEPTED);
            servletResponse.setContentType("text/perf-marker-stream");
            try {
                // Commit status and response headers now, rather than waiting
                // for first performance marker or result line.
                servletResponse.flushBuffer();
            } catch (IOException e) {
                LOGGER.warn("Unable to send response status and headers.");
            }

            /* Start async processing: no more exceptions! */

            HttpServletRequest servletRequest = ServletRequest.getRequest();
            _async = servletRequest.startAsync();
            _async.setTimeout(0); // Disable timeout as we don't know how long we'll take.

            if (_direction == Direction.PULL && _wantDigest.isPresent()) {
                // Ensure this is called before any perf-marker data is sent.
                addTrailerCallback();
            }

            _sendingMarkers = _scheduler.scheduleAtFixedRate(this::generateMarker,
                    _performanceMarkerPeriod,
                    _performanceMarkerPeriod, MILLISECONDS);

            return _transferResult;
        }

        /**
         * Check that the client is still connected.  To be effective, the
         * Connector should make use of NIO (e.g., SelectChannelConnector or
         * SslSelectChannelConnector) and this method should be called after
         * output has been written to the client.
        */
        private void checkClientConnected()
        {
            if (!_endpoint.isOpen()) {
                CancelTransferMessage message =
                        new CancelTransferMessage(_id, DUMMY_LONG);
                message.setExplanation("client went away");
                try {
                    _transferManager.sendAndWait(message);

                    /* We don't explicitly finish the transfer, but wait for
                     * the transfer manager to send a message notifying us that
                     * the transfer has completed.
                     */
                } catch (MissingResourceCacheException e) {
                    /* Tried to cancel a transfer, but the transfer-manager
                     * reported there is no such transfer.  Either the transfer
                     * complete message was lost or the transfer-service was
                     * restarted.  As the client has cancelled the transfer and
                     * there is no transfer, we have nothing further to do.
                     */
                    completed("client went away, but failed to cancel transfer: "
                            + e.getMessage());
                } catch (NoRouteToCellException | CacheException e) {
                    LOGGER.error("Failed to cancel transfer id={}: {}", _id, e.toString());

                    // Our attempt to kill the transfer failed.  We leave the
                    // performance markers going as they will trigger further
                    // attempts to kill the transfer.
                } catch (InterruptedException e) {
                    completed("dCache is shutting down");
                }
            }
        }

        private IpProtocolInfo buildProtocolInfo() throws ErrorResponseException
        {
            int buffer = MiB.toBytes(1);

            InetSocketAddress address = new InetSocketAddress(_destination.getHost(),
                    URIs.portWithDefault(_destination));

            if (address.isUnresolved()) {
                String target = _direction == Direction.PULL ? "source" : "destination";
                throw new ErrorResponseException(Response.Status.SC_BAD_REQUEST, "Unknown " + target + " hostname");
            }

            Optional<ChecksumType> desiredChecksum = _wantDigest.flatMap(Checksums::parseWantDigest);

            switch (_type) {
            case GSIFTP:
                return new RemoteGsiftpTransferProtocolInfo("RemoteGsiftpTransfer",
                        1, 1, address, _destination.toASCIIString(), null,
                        null, buffer, MiB.toBytes(1), _privateKey, _certificateChain,
                        null, desiredChecksum);

            case HTTP:
                return new RemoteHttpDataTransferProtocolInfo("RemoteHttpDataTransfer",
                        1, 1, address, _destination.toASCIIString(),
                        _flags.contains(TransferFlag.REQUIRE_VERIFICATION),
                        _transferHeaders, desiredChecksum);

            case HTTPS:
                if (_source == CredentialSource.OIDC) {
                    return new RemoteHttpsDataTransferProtocolInfo("RemoteHttpsDataTransfer",
                            1, 1, address, _destination.toASCIIString(),
                            _flags.contains(TransferFlag.REQUIRE_VERIFICATION),
                            _transferHeaders, desiredChecksum, _oidCredential);
                } else {
                    return new RemoteHttpsDataTransferProtocolInfo("RemoteHttpsDataTransfer",
                            1, 1, address, _destination.toASCIIString(),
                            _flags.contains(TransferFlag.REQUIRE_VERIFICATION),
                            _transferHeaders, _privateKey, _certificateChain,
                            desiredChecksum);
                }
            }

            throw new RuntimeException("Unexpected TransferType: " + _type);
        }

        /**
         * Provide the trailers (headers that appear after the request).
         */
        private HttpFields getTrailers()
        {
            return _digestValue.map(v -> {
                                HttpFields fields = new HttpFields();
                                fields.put("Digest", v);
                                return fields;
                            })
                    .orElse(null);
        }

        private void fetchChecksums()
        {
            if (_direction == Direction.PULL && _wantDigest.isPresent()) {
                Optional<String> empty = Optional.empty();
                _digestValue = _wantDigest.map(h -> {
                            try {
                                FileAttributes attributes = _pnfs.getFileAttributes(_path, EnumSet.of(CHECKSUM));
                                return Checksums.digestHeader(h, attributes);
                            } catch (CacheException e) {
                                LOGGER.warn("Failed to acquire checksum of fetched file: {}", e.getMessage());
                                return empty;
                            }
                        }).orElse(empty);
            }
        }

        private void addTrailerCallback()
        {
            /*
             * According to RFC 7230, we should only return trailers if the
             * client indicates (with the TE header) that it can understand
             * them.
             *
             * Jetty currently doesn't do this, so we must, instead.
             */
            String te = ServletRequest.getRequest().getHeader("TE");
            if (te != null && Splitter.on(',').omitEmptyStrings().trimResults().splitToList(te).stream()
                    .anyMatch(v -> v.equals("trailers") || v.startsWith("trailers;q="))) {

                /* REVISIT: trailers are available with Servlet v4.0; however
                 * support for Servlet v4.0 is only scheduled for Jetty v10.
                 * Jetty does support trailers but with a prioprietary interface,
                 * requiring the following ugly code.
                 */
                HttpServletResponse response = (HttpServletResponse)_async.getResponse();
                while (response instanceof ServletResponseWrapper) {
                    response = (HttpServletResponse)((ServletResponseWrapper)response).getResponse();
                }
                ((org.eclipse.jetty.server.Response)response).setTrailers(this::getTrailers);
            }
        }

        private void addDigestResponseHeader(FileAttributes attributes)
        {
            HttpServletResponse response = ServletResponse.getResponse();

            switch (_direction) {
            case PULL:
                if (_wantDigest.isPresent()) {
                    response.setHeader("Trailer", "Digest");
                }
                break;

            case PUSH:
                _wantDigest.flatMap(h -> Checksums.digestHeader(h, attributes))
                        .ifPresent(v -> response.setHeader("Digest", v));
                break;
            }
        }

        private void completed(String transferError)
        {
            if (_transfers.remove(_id) == null) {
                // Something else called complete, so do nothing.
                return;
            }

            // Note: must be `false` as this method may be called by the task.
            _sendingMarkers.cancel(false);

            String error = transferError;
            if (transferError == null) {
                fetchChecksums();
            } else {
                if (_direction == Direction.PULL) {
                    error = deleteFile()
                            .map(e -> transferError + " (" + e + ")")
                            .orElse(transferError);
                }
            }

            sendResult(error);
            _transferResult.set(Optional.ofNullable(error));
            _async.complete();
        }

        private Optional<String> deleteFile()
        {
            try {
                /* There is a subtlety here: when pulling a remote file, a
                 * user may be using a macaroon that allows the UPLOAD
                 * activity but not the DELETE activity.  This will allow
                 * the transfer to start, provided the file did not already
                 * exist.
                 *
                 * Failed pull transfers are deleted.  However, if the
                 * macaroon does not allow the DELETE activity then the user
                 * cannot delete the incomplete file.
                 *
                 * It is better to provide consistent behaviour: that
                 * incomplete pull transfers are deleted.  Therefore the
                 * delete operation is make without any restriction.  To
                 * achieve this, we create a new PnfsHandler with any
                 * restrictions removed.
                 */
                PnfsHandler pnfs = new PnfsHandler(_pnfs, null);
                pnfs.deletePnfsEntry(_pnfsId, _path.toString(),
                        EnumSet.of(FileType.REGULAR), EnumSet.noneOf(FileAttribute.class));
            } catch (FileNotFoundCacheException e) {
                // This is OK: either a new upload has started or the user
                // has deleted the file some other way.
                LOGGER.debug("Failed to clear up after failed transfer: {}",
                        e.getMessage());
            } catch (CacheException e) {
                LOGGER.warn("Failed to clear up after failed transfer: {}",
                        e.getMessage());
                return Optional.of("failed to remove badly transferred file");
            }
            return Optional.empty();
        }

        private void sendResult(String result)
        {
            try {
                var out = _async.getResponse().getWriter();

                if (result == null) {
                    out.println("success: Created");
                } else {
                    out.println("failure: " + result);
                }
                out.flush();
            } catch (IOException e) {
                LOGGER.warn("Unable to get writer: {}", e.toString());
            }
        }

        private void generateMarker()
        {
            TransferStatusQueryMessage message =
                    new TransferStatusQueryMessage(_id);
            ListenableFuture<TransferStatusQueryMessage> future =
                    _transferManager.send(message, _performanceMarkerPeriod/2);

            int state = TransferManagerHandler.UNKNOWN_ID;
            IoJobInfo info = null;
            try {
                TransferStatusQueryMessage reply = CellStub.getMessage(future);
                state = reply.getState();
                info = reply.getMoverInfo();
            } catch (MissingResourceCacheException e) {
                /*  RemoteTransferManager claims not to know about this
                 *  transfer.  The most likely explanation is that the service
                 *  has been restarted.  If the pool has already accepted the
                 *  mover then the transfer will not be affected by this
                 *  restart; however, we now have no way to monitor the
                 *  progress or cancel it.  The best we can do is to tell the
                 *  client the transfer has failed.
                 */

                /* We wait for two failures as a work-around for the race
                 * between WebDAV processing a TransferCompleteMessage and the
                 * progress marker query.
                 */
                if (_transferReportedAsUnknown) {
                    completed("RemoteTransferManager restarted");
                }
                _transferReportedAsUnknown = true;
            } catch (NoRouteToCellException | CacheException e) {
                LOGGER.warn("Failed to fetch information for progress marker: {}",
                        e.getMessage());
            } catch (InterruptedException e) {
                completed("dCache is shutting down");
            }

            sendMarker(state, info);
            checkClientConnected();
       }


        /**
         * Print a performance marker on the reply channel that looks something
         * like:
         *
         *     Perf Marker
         *      Timestamp: 1360578938
         *      Stripe Index: 0
         *      Stripe Bytes Transferred: 49397760
         *      Total Stripe Count: 2
         *     End
         *
         */
        private void sendMarker(int state, IoJobInfo info)
        {
            try {
                var out = _async.getResponse().getWriter();

                out.println("Perf Marker");
                out.println("    Timestamp: " +
                        MILLISECONDS.toSeconds(System.currentTimeMillis()));
                out.println("    State: " + state);
                out.println("    State description: " + TransferManagerHandler.describeState(state));
                out.println("    Stripe Index: 0");
                if (info != null) {
                    out.println("    Stripe Start Time: " +
                            MILLISECONDS.toSeconds(info.getStartTime()));
                    out.println("    Stripe Last Transferred: " +
                            MILLISECONDS.toSeconds(info.getLastTransferred()));
                    out.println("    Stripe Transfer Time: " +
                            MILLISECONDS.toSeconds(info.getTransferTime()));
                    out.println("    Stripe Bytes Transferred: " +
                            info.getBytesTransferred());
                    out.println("    Stripe Status: " + info.getStatus());
                }
                out.println("    Total Stripe Count: 1");
                if (info != null) {
                    List<InetSocketAddress> connections = info.remoteConnections();
                    if (connections != null) {
                        out.println("    RemoteConnections: " + connections.stream()
                                .map(conn -> "tcp:" + InetAddresses.toUriString(conn.getAddress()) + ":" + conn.getPort())
                                .collect(Collectors.joining(",")));
                    }
                }
                out.println("End");
                out.flush();
            } catch (IOException e) {
                LOGGER.warn("Unable to get writer for sending performance marker.");
            }
        }
    }
}
