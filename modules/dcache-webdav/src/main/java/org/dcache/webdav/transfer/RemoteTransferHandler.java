/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Comparators.emptiesFirst;
import static diskCacheV111.services.TransferManagerHandler.INITIAL_STATE;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.TYPE;
import static org.dcache.util.ByteUnit.MiB;
import static org.dcache.util.Strings.humanReadableSize;
import static org.dcache.util.Strings.toThreeSigFig;
import static org.dcache.util.TimeUtils.TimeUnitFormat.SHORT;
import static org.dcache.util.TimeUtils.appendDuration;
import static org.dcache.webdav.transfer.CopyFilter.CredentialSource.GRIDSITE;
import static org.dcache.webdav.transfer.CopyFilter.CredentialSource.NONE;
import static org.dcache.webdav.transfer.CopyFilter.CredentialSource.OIDC;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.services.TransferManagerHandler;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.MissingResourceCacheException;
import diskCacheV111.util.NotDirCacheException;
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
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandException;
import dmg.util.CommandSyntaxException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import eu.emi.security.authn.x509.X509Credential;
import io.milton.http.Response;
import io.milton.http.Response.Status;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.URI;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import javax.security.auth.Subject;
import javax.servlet.ServletResponseWrapper;
import javax.servlet.http.HttpServletResponse;
import org.apache.curator.shaded.com.google.common.base.Splitter;
import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.OpenIdCredential;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.BoundedCachedExecutor;
import org.dcache.util.BoundedExecutor;
import org.dcache.util.ChecksumType;
import org.dcache.util.Checksums;
import org.dcache.util.ColumnWriter;
import org.dcache.util.ColumnWriter.TabulatedRow;
import org.dcache.util.Glob;
import org.dcache.util.NetworkUtils;
import org.dcache.util.Strings;
import org.dcache.util.URIs;
import org.dcache.vehicles.FileAttributes;
import org.dcache.webdav.transfer.CopyFilter.CredentialSource;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.HttpConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import static dmg.util.CommandException.checkCommand;

/**
 * This class provides the basis for interactions with the remotetransfer service.  It is used by
 * the CopyFilter to manage a requested transfer and to provide feedback on that transfer in the
 * form of performance markers.
 * <p>
 * The performance markers are similar to those provided during an FTP transfer.  They have the
 * form:
 * <code>
 * Perf Marker Timestamp: 1360578938 Stripe Index: 0 Stripe Bytes Transferred: 49397760 Total Stripe
 * Count: 2 End
 * </code>
 * <p>
 * Once the transfer has completed successfully, {@code success: Created} is reported.  On failure
 * {@code failure: <explanation>} is returned.
 * <p>
 * Although the third-party transfer protocol, described in CopyFilter is documented as supporting
 * 'https' URIs, this implementation supports different transports for the third-party transfer.
 */
public class RemoteTransferHandler implements CellMessageReceiver, CellCommandListener {

    private static int threadCount = 1;
    private static synchronized int nextThreadCount() {
        return threadCount++;
    }
    /**
     * The different directions that the data will travel.
     */
    public enum Direction {
        /**
         * Request to pull data from remote site.
         */
        PULL("Source"),

        /**
         * Request to push data to some remote site.
         */
        PUSH("Destination");

        private final String header;

        Direction(String header) {
            this.header = header;
        }

        public String getHeaderName() {
            return header;
        }
    }


    /**
     * The different transport schemes supported.
     */
    public enum TransferType {
        GSIFTP("gsiftp", EnumSet.of(GRIDSITE)),
        HTTP("http", EnumSet.of(NONE)),
        HTTPS("https", EnumSet.of(GRIDSITE, OIDC, NONE));

        private static final ImmutableMap<String, TransferType> BY_SCHEME =
              ImmutableMap.of("gsiftp", GSIFTP, "http", HTTP, "https", HTTPS);

        private final EnumSet<CredentialSource> _supported;
        private final String _scheme;

        TransferType(String scheme, EnumSet<CredentialSource> supportedSources) {
            _supported = EnumSet.copyOf(supportedSources);
            _scheme = scheme;
        }

        public boolean isSupported(CredentialSource source) {
            return _supported.contains(source);
        }

        public String getScheme() {
            return _scheme;
        }

        public static TransferType fromScheme(String scheme) {
            return BY_SCHEME.get(scheme.toLowerCase());
        }

        public static Set<String> validSchemes() {
            return BY_SCHEME.keySet();
        }
    }

    private enum TransferFlag {
        REQUIRE_VERIFICATION
    }

    private static final Set<AccessMask> READ_ACCESS_MASK =
          EnumSet.of(AccessMask.READ_DATA);

    private static final Logger LOGGER =
          LoggerFactory.getLogger(RemoteTransferHandler.class);
    private static final long DUMMY_LONG = 0;
    private static final String REQUEST_HEADER_TRANSFER_HEADER_PREFIX =
          "transferheader";

    private final HashMap<Long, RemoteTransfer> _transfers = new HashMap<>();
    private final BoundedExecutor _activity =
          new BoundedCachedExecutor(r -> new Thread(r, "transfer-finaliser-" + nextThreadCount()), 1);

    private long _performanceMarkerPeriod;
    private CellStub _transferManager;
    private PnfsHandler _pnfs;

    @Required
    public void setTransferManagerStub(CellStub stub) {
        _transferManager = stub;
    }

    @Required
    public void setPnfsStub(CellStub stub) {
        _pnfs = new PnfsHandler(stub);
    }

    @Required
    public void setPerformanceMarkerPeroid(long peroid) {
        _performanceMarkerPeriod = peroid;
    }

    public long getPerformanceMarkerPeroid() {
        return _performanceMarkerPeriod;
    }

    public void setMaxConcurrentFinalisers(int count) {
        _activity.setMaximumPoolSize(count);
    }

    private enum IPFamilyMatcher {
        IPv4 {
            @Override
            public boolean matches(InetAddress addr) {
                return addr instanceof Inet4Address;
            }
        },

        IPv6 {
            @Override
            public boolean matches(InetAddress addr) {
                return addr instanceof Inet6Address;
            }
        };

        public abstract boolean matches(InetAddress addr);
    }

    @Command(name = "http-tpc finaliser info", hint="query finaliser status",
            description = "Provides information about transfer finalisation.")
    public class HttpTpcFinaliserInfoCommand implements Callable<String> {
        @Override
        public String call() throws Exception {
            StringBuilder sb = new StringBuilder();
            sb.append("Max threads: ").append(_activity.getMaximumPoolSize()).append('\n');
            sb.append("Current threads: ").append(_activity.getThreadCount()).append('\n');
            sb.append("Queued tasks: ").append(_activity.getWorkQueueSize()).append('\n');
            return sb.toString();
        }
    }

    @Command(name = "http-tpc finaliser max threads", hint="update finaliser max threads",
            description = "Updates the maximum number of threads that are satisfying transfer finalisation.")
    public class HttpTpcFinaliserMaxThreadsCommand implements Callable<String> {
        @Argument(usage="The new maximum number of threads", metaVar="threads")
        int maxThreads;

        @Override
        public String call() throws Exception {
            checkCommand(maxThreads >= 1, "Number of threads must be 1 or more");
            _activity.setMaximumPoolSize(maxThreads);
            return "";
        }
    }

    @Command(name = "http-tpc ls", hint = "list current transfers",
          description = "Provides a summary of all currently HTTP-TPC transfers.")
    public class HttpTpcLsCommand implements Callable<String> {

        @Option(name = "t", usage = "Show timing information.", category = "Field options")
        boolean showTiming;

        @Option(name = "n", usage = "Show network related information.",
              category = "Field options")
        boolean showNetwork;

        @Option(name = "l", usage = "Show the local path of the transfer.",
              category = "Field options")
        boolean showLocalPath;

        @Option(name = "r", usage = "Show the remote path for this transfer.",
              category = "Field options")
        boolean showRemotePath;

        @Option(name = "a", usage = "Show all available information about"
              + " transfers.  This is equivalent to specifying \"-t -n -l"
              + " -r\"", category = "Field options")
        boolean showAll;

        @Option(name = "pool", usage = "Select transfers involving a pool that"
              + " matches this glob pattern.", category = "Filter options")
        Glob poolFilter;

        @Option(name = "host", usage = "Select transfers involving a remote host"
              + " that matches this glob pattern.", category = "Filter options")
        Glob hostFilter;

        @Option(name = "dir", usage = "Select transfers with the specified"
              + " direction.", category = "Filter options")
        Direction direction;

        /*  REVISIT: we can't use an enum directly when definint the 'ip'
            option because of a hard-coded `String#toUpperCase` on the option's
            argument/value in AnnotatedCommandExecutor.EnumTypeConverter.  We
            also can't use "valueSpec" because upper case letters are always
            converted to place-holder (e.g., "IPv4" is written as "<ip>v4" in
            the help text).
         */
        @Option(name = "ip", usage = "Select transfers using at least one"
              + " network connection with the specified IP address family."
              + "  Valid values are \"IPv4\" or \"IPv6\"",
              category = "Filter options")
        String ipFamily;

        @Option(name = "local-path", usage = "Select transfers where the local path"
              + " matches this pattern.", category = "Filter options")
        Glob localPathFilter;

        @Option(name = "remote-path", usage = "Select transfers where the remote"
              + " path matches this pattern.", category = "Filter options")
        Glob remotePathFilter;

        @Option(name = "sort-by", usage = "How to order the output.  The available"
              + " options are described below.\n"
              + "    \"host\" sorts remote host.\n"
              + "    \"pool\" sorts by pool name, with any transfers not yet"
              + " assigned to a pool are shown first.\n"
              + "    \"lifetime\" sorts by the request's lifetime, showing"
              + " the most recent request first.\n"
              + "    \"running\" sorts by the time active in increasing"
              + " order, any queued transfers are shown first.",
              valueSpec = "host|pool|lifetime|running",
              category = "Ordering options")
        String sort;

        private ColumnWriter output = new ColumnWriter();

        @Override
        public String call() throws CommandException {
            if (showAll) {
                showTiming = true;
                showNetwork = true;
                showLocalPath = true;
                showRemotePath = true;
            }

            buildHeaders();

            synchronized (_transfers) {
                _transfers.values().stream()
                      .filter(buildFilter())
                      .sorted(buildComparator())
                      .forEachOrdered(this::printTransfer);
            }

            return output.toString();
        }

        private void buildHeaders() {
            output = output.header("ID").left("id");

            if (showTiming) {
                output.space().header("Lifetime").left("lifetime")
                      .space().header("Queued").left("queued")
                      .space().header("Running").left("running");
            }

            output.space().header("Dirn").left("direction")
                  .space().header("Pool").left("pool");

            if (showNetwork) {
                output.space().header("IP").left("ip")
                      .space().header("Transferred").left("transferred");
            }

            if (showLocalPath) {
                output.space().header("Local path").left("local-path");
            }

            output.space().header("Remote host").left("host");

            if (showRemotePath) {
                output.space().header("Remote path").left("remote-path");
            }
        }

        private Predicate<RemoteTransfer> buildFilter() throws CommandSyntaxException {
            Predicate<RemoteTransfer> p = t -> true;

            if (poolFilter != null) {
                p = p.and(t -> t._pool.map(poolFilter::matches).orElse(Boolean.FALSE));
            }

            if (hostFilter != null) {
                p = p.and(t -> hostFilter.matches(t._destination.getHost()));
            }

            if (direction != null) {
                p = p.and(t -> t._direction == direction);
            }

            if (ipFamily != null) {
                IPFamilyMatcher ipMatcher = asIPFamilyMatcher(ipFamily);
                p = p.and(t -> t._lastInfo
                      .map(IoJobInfo::remoteConnections)
                      .filter(Objects::nonNull)
                      .map(a -> a.stream()
                            .map(InetSocketAddress::getAddress)
                            .anyMatch(ipMatcher::matches))
                      .orElse(Boolean.FALSE));
            }

            if (localPathFilter != null) {
                p = p.and(t -> localPathFilter.matches(t._path.toString()));
            }

            if (remotePathFilter != null) {
                p = p.and(t -> remotePathFilter.matches(t._destination.getPath()));
            }

            return p;
        }

        private IPFamilyMatcher asIPFamilyMatcher(String id)
              throws CommandSyntaxException {
            try {
                return IPFamilyMatcher.valueOf(id);
            } catch (IllegalArgumentException e) {
                throw new CommandSyntaxException("Invalid 'ip' option \"" + id
                      + "\": valid values are \"IPv4\" and \"IPv6\"");
            }
        }

        private Comparator<RemoteTransfer> buildComparator()
              throws CommandSyntaxException {
            if (sort == null) {
                return Comparator.comparingLong(t -> t._id);
            }

            Comparator<RemoteTransfer> primary;
            switch (sort) {
                case "pool":
                    primary = Comparator.comparing(t -> t._pool,
                          emptiesFirst(String::compareTo));
                    break;
                case "host":
                    primary = Comparator.comparing(t -> t._destination.getHost(),
                          Comparator.nullsLast(String::compareToIgnoreCase));
                    break;
                case "lifetime":
                    primary = Comparator.comparing(t -> t._whenSubmitted,
                          Comparator.reverseOrder());
                    break;
                case "running":
                    primary = Comparator.comparing(t -> t._transferStarted,
                          emptiesFirst(Comparator.reverseOrder()));
                    break;
                default:
                    throw new CommandSyntaxException("Invalid 'sort-by' option"
                          + " argument \"" + sort + "\"");
            }

            return primary.thenComparingLong(t -> t._id);
        }

        /**
         * Return the (expected) total number of bytes that will be transferred for this operation.
         * Returns -1 if the value is unknown. REVISIT: consider returning OptionalLong.
         */
        private long expectedTotalTransferSize(RemoteTransfer transfer) {
            Long sizeFromPool = transfer._lastInfo
                  .map(IoJobInfo::requestedBytes)
                  .orElse(null);

            if (sizeFromPool != null) {
                return sizeFromPool;
            }

            return transfer._direction == Direction.PUSH
                  ? transfer._size
                  : -1;
        }

        private void printTransfer(RemoteTransfer transfer) {
            TabulatedRow row = output.row();

            row.value("id", transfer._id)
                  .value("direction", transfer._direction)
                  .value("host", transfer._destination.getHost())
                  .value("pool", transfer._pool.orElse("-"));

            if (showLocalPath) {
                row.value("local-path", transfer._path.toString());
            }

            if (showRemotePath) {
                row.value("remote-path", transfer._destination.getPath());
            }

            if (showTiming) {
                StringBuilder lifetime = appendDuration(new StringBuilder(),
                      Duration.between(transfer._whenSubmitted, Instant.now()),
                      SHORT);
                row.value("lifetime", lifetime);

                Duration queued = Duration.between(transfer._whenSubmitted,
                      transfer._transferStarted.orElseGet(() -> Instant.now()));
                StringBuilder queueDescription = appendDuration(new StringBuilder(),
                      queued, SHORT);
                row.value("queued", queueDescription);

                Optional<String> running = transfer._transferStarted
                      .map(i -> Duration.between(i, Instant.now()))
                      .map(d -> appendDuration(new StringBuilder(), d, SHORT))
                      .map(Object::toString);
                row.value("running", running.orElse("-"));
            }

            if (showNetwork) {
                Optional<Long> transferred = transfer._lastInfo.map(IoJobInfo::getBytesTransferred);

                long total = expectedTotalTransferSize(transfer);
                Optional<String> transferDescription = total == -1
                      ? transferred.map(Strings::humanReadableSize)
                      : transferred.map(i -> {
                          String percent = toThreeSigFig(100 * i / (double) total, 1000);
                          return humanReadableSize(i) + " (" + percent + "%" + ")";
                      });
                row.value("transferred", transferDescription.orElse("-"));

                Optional<String> ipProtocols = transfer._lastInfo
                      .flatMap(i -> Optional.ofNullable(i.remoteConnections()))
                      .map(l -> l.stream()
                            .map(a -> NetworkUtils.getProtocolFamily(a.getAddress()))
                            .distinct()
                            .sorted()
                            .map(f -> f == StandardProtocolFamily.INET ? "IPv4" : "IPv6")
                            .collect(Collectors.joining(",")));
                row.value("ip", ipProtocols.orElse("-"));
            }
        }
    }

    @PreDestroy
    public void stop() {
        _activity.shutdown();
    }

    /**
     * Start a transfer and block until that transfer is complete.
     *
     * @return a description of the error, if there was a problem.
     */
    public Optional<String> acceptRequest(Response response, Map<String, String> requestHeaders,
          Subject subject, Restriction restriction, FsPath path, URI remote,
          Object credential, Direction direction, boolean verification,
          boolean overwriteAllowed, Optional<String> wantDigest)
          throws ErrorResponseException, InterruptedException {
        checkArgument(credential == null
                    || credential instanceof X509Credential
                    || credential instanceof OpenIdCredential,
              "Credential not supported for Third-Party Transfer");
        EnumSet<TransferFlag> flags = verification
              ? EnumSet.of(TransferFlag.REQUIRE_VERIFICATION)
              : EnumSet.noneOf(TransferFlag.class);
        ImmutableMap<String, String> transferHeaders = buildTransferHeaders(requestHeaders);
        RemoteTransfer transfer = new RemoteTransfer(response.getOutputStream(), subject,
              restriction,
              path, remote, credential, flags, transferHeaders, direction,
              overwriteAllowed, wantDigest);

        long id;

        synchronized (_transfers) {
            id = transfer.start();
            response.setStatus(Status.SC_ACCEPTED);
            response.setContentTypeHeader("text/perf-marker-stream");
            _transfers.put(id, transfer);
        }

        try {
            transfer.awaitCompletion();
        } finally {
            synchronized (_transfers) {
                _transfers.remove(id);
            }
        }

        return Optional.ofNullable(transfer._problem);
    }

    private ImmutableMap<String, String> buildTransferHeaders(Map<String, String> requestHeaders) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        for (Map.Entry<String, String> header : requestHeaders.entrySet()) {
            String key = header.getKey();
            if (key.toLowerCase().startsWith(REQUEST_HEADER_TRANSFER_HEADER_PREFIX)) {
                builder.put(key.substring(REQUEST_HEADER_TRANSFER_HEADER_PREFIX.length()),
                      header.getValue());
            }
        }

        return builder.build();
    }


    public void messageArrived(TransferCompleteMessage message) {
        synchronized (_transfers) {
            RemoteTransfer transfer = _transfers.get(message.getId());
            if (transfer != null) {
                _activity.execute(() -> transfer.success());
            }
        }
    }

    public void messageArrived(TransferFailedMessage message) {
        synchronized (_transfers) {
            RemoteTransfer transfer = _transfers.get(message.getId());
            if (transfer != null) {
                String error = String.valueOf(message.getErrorObject());
                _activity.execute(() -> transfer.failure(error));
            }
        }
    }

    /**
     * Class that represents a client's request to transfer a file to some remote server.
     * <p>
     * This class needs to be aware of the client closing its end of the TCP connection while the
     * transfer underway.  In the protocol, this is used to indicate that the transfer should be
     * cancelled.  Unfortunately, there is no container-independent mechanism for discovering this,
     * so Jetty-specific code is needed.
     */
    private class RemoteTransfer {

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
        private final PrintWriter _out;
        private final EnumSet<TransferFlag> _flags;
        private final ImmutableMap<String, String> _transferHeaders;
        private final Direction _direction;
        private final boolean _overwriteAllowed;
        private final Optional<String> _wantDigest;
        private final PnfsHandler _pnfs;
        private final Instant _whenSubmitted = Instant.now();
        private String _problem;
        private long _id;
        private final EndPoint _endpoint = HttpConnection.getCurrentConnection().getEndPoint();

        private int _lastState = INITIAL_STATE;
        private Optional<IoJobInfo> _lastInfo = Optional.empty();
        private Optional<Instant> _transferStarted = Optional.empty();
        private PnfsId _pnfsId;
        private boolean _finished;
        private Optional<String> _digestValue;
        private Optional<String> _pool = Optional.empty();
        private boolean _transferReportedAsUnknown;
        private long _size;

        public RemoteTransfer(OutputStream out, Subject subject, Restriction restriction,
              FsPath path, URI destination, @Nullable Object credential,
              EnumSet<TransferFlag> flags, ImmutableMap<String, String> transferHeaders,
              Direction direction, boolean overwriteAllowed, Optional<String> wantDigest)
              throws ErrorResponseException {
            _subject = subject;
            _restriction = restriction;
            _pnfs = new PnfsHandler(RemoteTransferHandler.this._pnfs, _subject, _restriction);
            _path = path;
            _destination = destination;
            _type = TransferType.fromScheme(destination.getScheme());
            if (credential instanceof X509Credential) {
                _privateKey = ((X509Credential) credential).getKey();
                _certificateChain = ((X509Credential) credential).getCertificateChain();
                _source = CredentialSource.GRIDSITE;
                _oidCredential = null;
            } else if (credential instanceof OpenIdCredential) {
                _privateKey = null;
                _certificateChain = null;
                _source = CredentialSource.OIDC;
                _oidCredential = (OpenIdCredential) credential;
            } else {
                _privateKey = null;
                _certificateChain = null;
                _source = null;
                _oidCredential = null;
            }
            _out = new PrintWriter(out);
            _flags = flags;
            _transferHeaders = transferHeaders;
            _direction = direction;
            _overwriteAllowed = overwriteAllowed;
            _wantDigest = wantDigest;
        }


        /**
         * Obtain the PnfsId of the local file, creating it as necessary.
         */
        private FileAttributes resolvePath() throws ErrorResponseException {
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
                                throw new ErrorResponseException(Response.Status.SC_BAD_REQUEST,
                                      "Not a file");
                            }

                            if (!attributes.isDefined(SIZE)) {
                                throw new ErrorResponseException(Response.Status.SC_CONFLICT,
                                      "File upload in progress");
                            }
                            _size = attributes.getSize();

                            return attributes;
                        } catch (FileNotFoundCacheException e) {
                            throw new ErrorResponseException(Response.Status.SC_NOT_FOUND,
                                  "no such file");
                        }

                    case PULL:
                        PnfsCreateEntryMessage msg;
                        FileAttributes attributes = FileAttributes.of()
                              .fileType(FileType.REGULAR)
                              .xattr("xdg.origin.url", _destination.toASCIIString())
                              .build();
                        try {
                            msg = _pnfs.createPnfsEntry(_path.toString(), attributes);
                        } catch (FileNotFoundCacheException | NotDirCacheException e) {
                            // Parent directory missing or parent is a file.
                            throw new ErrorResponseException(Response.Status.SC_BAD_REQUEST, e.getMessage());
                        } catch (FileExistsCacheException e) {
                            /* REVISIT: This should be moved to PnfsManager with a
                             * flag in the PnfsCreateEntryMessage.
                             */
                            if (!_overwriteAllowed) {
                                throw e;
                            }
                            _pnfs.deletePnfsEntry(_path.toString(), EnumSet.of(FileType.REGULAR));
                            msg = _pnfs.createPnfsEntry(_path.toString(), attributes);
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


        private long start() throws ErrorResponseException, InterruptedException {
            FileAttributes attributes = resolvePath();
            _pnfsId = attributes.getPnfsId();

            boolean isStore = _direction == Direction.PULL;
            RemoteTransferManagerMessage message =
                  new RemoteTransferManagerMessage(_destination, _path, isStore,
                        DUMMY_LONG, buildProtocolInfo());

            message.setSubject(_subject);
            message.setRestriction(_restriction);
            message.setPnfsId(_pnfsId);
            try {
                _id = _transferManager.sendAndWait(message).getId();
                addDigestResponseHeader(attributes);
                return _id;
            } catch (NoRouteToCellException | TimeoutCacheException e) {
                LOGGER.error("Failed to send request to transfer manager: {}", e.getMessage());
                throw new ErrorResponseException(Response.Status.SC_INTERNAL_SERVER_ERROR,
                      "transfer service unavailable");
            } catch (CacheException e) {
                LOGGER.error("Error from transfer manager: {}", e.getMessage());
                throw new ErrorResponseException(Response.Status.SC_INTERNAL_SERVER_ERROR,
                      "transfer not accepted: " + e.getMessage());
            }
        }

        /**
         * Check that the client is still connected.  To be effective, the Connector should make use
         * of NIO (e.g., SelectChannelConnector or SslSelectChannelConnector) and this method should
         * be called after output has been written to the client.
         */
        private void checkClientConnected() {
            if (!_endpoint.isOpen()) {
                CancelTransferMessage message =
                      new CancelTransferMessage(_id, DUMMY_LONG);
                message.setExplanation("client went away");
                try {
                    _transferManager.sendAndWait(message);
                } catch (MissingResourceCacheException e) {
                    /* Tried to cancel a transfer, but the transfer-manager
                     * reported there is no such transfer.  Either the transfer
                     * complete message was lost or the transfer-service was
                     * restarted.  As the client has cancelled the transfer and
                     * there is no transfer, we have nothing further to do.
                     */
                    failure("client went away, but failed to cancel transfer: "
                          + e.getMessage());
                } catch (NoRouteToCellException | CacheException e) {
                    LOGGER.error("Failed to cancel transfer id={}: {}", _id, e.toString());

                    // Our attempt to kill the transfer failed.  We leave the
                    // performance markers going as they will trigger further
                    // attempts to kill the transfer.
                } catch (InterruptedException e) {
                    // Do nothing: this dCache domain is shutting down.
                }
            }
        }

        private IpProtocolInfo buildProtocolInfo() throws ErrorResponseException {
            int buffer = MiB.toBytes(1);

            InetSocketAddress address = new InetSocketAddress(_destination.getHost(),
                  URIs.portWithDefault(_destination));

            if (address.isUnresolved()) {
                String target = _direction == Direction.PULL ? "source" : "destination";
                throw new ErrorResponseException(Response.Status.SC_BAD_REQUEST,
                      "Unknown " + target + " hostname");
            }

            Optional<ChecksumType> desiredChecksum = _wantDigest.flatMap(
                  Checksums::parseWantDigest);

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
        private HttpFields getTrailers() {
            return _digestValue.map(v -> {
                      HttpFields fields = new HttpFields();
                      fields.put("Digest", v);
                      return fields;
                  })
                  .orElse(null);
        }

        private void fetchChecksums() {
            Optional<String> empty = Optional.empty();
            _digestValue = _wantDigest.map(h -> {
                try {
                    FileAttributes attributes = _pnfs.getFileAttributes(_path,
                          EnumSet.of(CHECKSUM));
                    return Checksums.digestHeader(h, attributes);
                } catch (CacheException e) {
                    LOGGER.warn("Failed to acquire checksum of fetched file: {}", e.getMessage());
                    return empty;
                }
            }).orElse(empty);
        }

        private void addTrailerCallback() {
            /*
             * According to RFC 7230, we should only return trailers if the
             * client indicates (with the TE header) that it can understand
             * them.
             *
             * Jetty currently doesn't do this, so we must, instead.
             */
            String te = ServletRequest.getRequest().getHeader("TE");
            if (te != null && Splitter.on(',').omitEmptyStrings().trimResults().splitToList(te)
                  .stream()
                  .anyMatch(v -> v.equals("trailers") || v.startsWith("trailers;q="))) {

                /* REVISIT: trailers are available with Servlet v4.0; however
                 * support for Servlet v4.0 is only scheduled for Jetty v10.
                 * Jetty does support trailers but with a prioprietary interface,
                 * requiring the following ugly code.
                 */
                HttpServletResponse response = ServletResponse.getResponse();
                while (response instanceof ServletResponseWrapper) {
                    response = (HttpServletResponse) ((ServletResponseWrapper) response).getResponse();
                }
                ((org.eclipse.jetty.server.Response) response).setTrailers(this::getTrailers);
            }
        }

        private void addDigestResponseHeader(FileAttributes attributes) {
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

        public synchronized void success() {
            _problem = null;
            _finished = true;
            notifyAll();
        }

        public synchronized void failure(String explanation) {
            _problem = explanation;
            _finished = true;
            notifyAll();
            if (_direction == Direction.PULL) {
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
                    _problem += " (failed to remove badly transferred file)";
                }
            }
        }

        public synchronized void awaitCompletion() throws InterruptedException {
            if (_direction == Direction.PULL && _wantDigest.isPresent()) {
                // Ensure this is called before any perf-marker data is sent.
                addTrailerCallback();
            }

            do {
                generateMarker();

                wait(_performanceMarkerPeriod);
            } while (!_finished);

            if (_problem == null) {
                _out.println("success: Created");

                if (_direction == Direction.PULL && _wantDigest.isPresent()) {
                    fetchChecksums();
                }

            } else {
                _out.println("failure: " + _problem);
            }
            _out.flush();
        }

        private void generateMarker() throws InterruptedException {
            TransferStatusQueryMessage message =
                  new TransferStatusQueryMessage(_id);
            ListenableFuture<TransferStatusQueryMessage> future =
                  _transferManager.send(message, _performanceMarkerPeriod / 2);

            int state = TransferManagerHandler.UNKNOWN_ID;
            IoJobInfo info = null;
            TransferStatusQueryMessage reply = null;
            try {
                reply = CellStub.getMessage(future);
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
                    failure("RemoteTransferManager restarted");
                }
                _transferReportedAsUnknown = true;
            } catch (NoRouteToCellException | CacheException e) {
                LOGGER.warn("Failed to fetch information for progress marker: {}",
                      e.getMessage());
            }

            _lastState = state;
            _lastInfo = Optional.ofNullable(info);
            if (info != null) {
                if (!_transferStarted.isPresent() && info.getTransferTime() > 0) {
                    Instant started = Instant.now().minus(info.getTransferTime(), MILLIS);
                    _transferStarted = Optional.of(started);
                }

                if (!_pool.isPresent() && reply != null && reply.getPool() != null) {
                    _pool = Optional.of(reply.getPool());
                }
            }

            sendMarker(state, info);
            checkClientConnected();
        }


        /**
         * Print a performance marker on the reply channel that looks something like:
         * <p>
         * Perf Marker Timestamp: 1360578938 Stripe Index: 0 Stripe Bytes Transferred: 49397760
         * Total Stripe Count: 2 End
         */
        public void sendMarker(int state, IoJobInfo info) {
            _out.println("Perf Marker");
            _out.println("    Timestamp: " +
                  MILLISECONDS.toSeconds(System.currentTimeMillis()));
            _out.println("    State: " + state);
            _out.println("    State description: " + TransferManagerHandler.describeState(state));
            _out.println("    Stripe Index: 0");
            if (info != null) {
                _out.println("    Stripe Start Time: " +
                      MILLISECONDS.toSeconds(info.getStartTime()));
                _out.println("    Stripe Last Transferred: " +
                      MILLISECONDS.toSeconds(info.getLastTransferred()));
                _out.println("    Stripe Transfer Time: " +
                      MILLISECONDS.toSeconds(info.getTransferTime()));
                _out.println("    Stripe Bytes Transferred: " +
                      info.getBytesTransferred());
                _out.println("    Stripe Status: " + info.getStatus());
            }
            _out.println("    Total Stripe Count: 1");
            if (info != null) {
                List<InetSocketAddress> connections = info.remoteConnections();
                if (connections != null) {
                    _out.println("    RemoteConnections: " + connections.stream()
                          .map(conn -> "tcp:" + InetAddresses.toUriString(conn.getAddress()) + ":"
                                + conn.getPort())
                          .collect(Collectors.joining(",")));
                }
            }
            _out.println("End");
            _out.flush();
        }
    }
}
