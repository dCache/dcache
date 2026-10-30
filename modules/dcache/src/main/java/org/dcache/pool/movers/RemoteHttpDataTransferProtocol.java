package org.dcache.pool.movers;

import static diskCacheV111.util.ThirdPartyTransferFailedCacheException.checkThirdPartyTransferSuccessful;
import static java.util.Objects.requireNonNull;
import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.util.Exceptions.genericCheck;
import static org.dcache.util.Exceptions.messageOrClassName;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ThirdPartyTransferFailedCacheException;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import javax.security.auth.Subject;
import org.apache.http.impl.client.CloseableHttpClient;
import org.dcache.http.tpc.RemoteHttpTransferClient;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements transfers of data between a pool and some remote HTTP server.  Both writing
 * data into dCache and reading from dCache are supported (HTTP GET and PUT respectively).
 * On-transfer checksum calculation is supported.  Support is also included for RFC-3230, which
 * allows the remote server to specify one or more checksums as part of a response to a GET or HEAD
 * request.
 * <p>
 * If the remote server supports RFC-3230 then this is used to discover the remote file's
 * checksum(s).  Using this, dCache will always try to verify the transfer was successful.  dCache
 * will be unable to do this only if the file is sent to a remote server that provides a set of
 * checksums that doesn't overlap with the set of checksums dCache knows for this file.
 * <p>
 * The mover supports a require-checksum-verification flag.
 * <p>
 * When enabled, the integrity of the transferred data must be verified by checking a remote
 * supplied checksum matches one known locally (either calculated as part of the transfer or already
 * know for this file).  If the flag is enabled and verification is impossible (e.g., the remote
 * server supplied no checksums) then the transfer will fail.
 * <p>
 * If require-checksum-verification flag is disabled then a transfer will not fail if the remote
 * server supplies no checksum; however, if checksums are supplied then they are checked against
 * locally known checksums (either calculated as part of the transfer or already known for this
 * file) and a mismatch will fail the transfer.
 * <p>
 * <p>
 * WRITE REQUESTS
 * <p>
 * The pool accepts only a single client-supplied (i.e., from the remote server) checksum value.
 * Therefore if the remote server supplies more than one checksum then one is selected: either
 * matching the pool's on-transfer checksum choice or a hard-coded preference list.
 * <p>
 * If require-checksum-verification is enabled and the remote server supplied no checksum that
 * dCache understands, or the server doesn't support RFC-3230, then the transfer fails.
 * <p>
 * <p>
 * READ REQUESTS
 * <p>
 * When the request is for reading data (an HTTP PUT request) then the mover will copy the file to
 * the remote server and try to verify that the file arrived OK using the HTTP HEAD command.  If the
 * HEAD request fails or the Content-Length value is wrong, or (if the remote server supports
 * RFC-3230) the supplied checksums indicate data corruption then the mover will fail the transfer.
 * <p>
 * If checksum-verification-required is enabled and the remote server does not support RFC-3230 or
 * none of the checksums provided by the remote server were calculated using the same algorithm as a
 * known checksum for this file then the mover will fail the transfer.
 * <p>
 * If checksum-verification-required is disabled then a lack of checksum verification does not fail
 * the transfer.
 * <p>
 * If the PUT request fails, for whatever reason, then the mover will attempt to clear up the
 * transfer by deleting the remote copy via the HTTP DELETE command.  If the cleanup is successful
 * then the error triggering the cleanup is reported.  If the cleanup is unsuccessful then an error
 * is reported containing both the error in removing the remote file and the error that triggered
 * the delete.
 */
public class RemoteHttpDataTransferProtocol implements MoverProtocol,
      ChecksumMover {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(RemoteHttpDataTransferProtocol.class);

    private volatile MoverChannel<RemoteHttpDataTransferProtocolInfo> _channel;
    private Consumer<Checksum> _integrityChecker;

    private final CloseableHttpClient _client;

    private volatile RemoteHttpTransferClient _transfer;

    private InetSocketAddress _localEndpoint;
    private final TransferLifeCycle _transferLifeCycle;
    private Subject _subject;
    private boolean _startMarkerSent;

    public RemoteHttpDataTransferProtocol(CloseableHttpClient client,
          TransferLifeCycle transferLifeCycle) {
        _client = requireNonNull(client);
        _transferLifeCycle = requireNonNull(transferLifeCycle);
    }

    public void setSubject(Subject subject) {
        _subject = subject;
    }

    private static void checkThat(boolean isOk, String message) throws CacheException {
        genericCheck(isOk, CacheException::new, message);
    }

    @Override
    public void acceptIntegrityChecker(Consumer<Checksum> integrityChecker) {
        _integrityChecker = integrityChecker;
    }

    @Override
    public void runIO(FileAttributes attributes, RepositoryChannel channel,
          ProtocolInfo genericInfo, Set<? extends OpenOption> access)
          throws CacheException, IOException, InterruptedException {
        LOGGER.debug("info={}, attributes={},  access={}", genericInfo,
              attributes, access);
        RemoteHttpDataTransferProtocolInfo info =
              (RemoteHttpDataTransferProtocolInfo) genericInfo;
        MoverChannel<RemoteHttpDataTransferProtocolInfo> moverChannel =
              new MoverChannel<>(access, attributes, info, channel);
        _channel = moverChannel;
        _transfer = new RemoteHttpTransferClient(_client, info, this::onConnected);

        channel.optionallyAs(ChecksumChannel.class).ifPresent(c -> {
            info.getDesiredChecksums().forEach(t -> {
                try {
                    c.addType(t);
                } catch (IOException e) {
                    LOGGER.warn("Unable to calculate checksum {}: {}",
                          t, messageOrClassName(e));
                }
            });
        });

        try {
            if (access.contains(StandardOpenOption.WRITE)) {
                receiveFile();
            } else {
                checkThat(!info.isVerificationRequired() || attributes.isDefined(CHECKSUM),
                      "checksum verification failed: file has no checksum");
                sendAndCheckFile(attributes);
            }
        } finally {
            afterTransfer();
        }
    }

    /**
     * This method is guaranteed to be called after all activity related to this transfer has
     * completed.
     */
    protected void afterTransfer() {}

    private void onConnected() {
        _localEndpoint = _transfer.localAddress().orElse(null);
        startFlowMarker();
    }

    private RemoteHttpTransferClient.Progress progress() {
        MoverChannel<RemoteHttpDataTransferProtocolInfo> channel = _channel;
        return new RemoteHttpTransferClient.Progress() {
            @Override
            public long getBytesTransferred() {
                return channel.getBytesTransferred();
            }

            @Override
            public long getSize() throws IOException {
                return channel.size();
            }
        };
    }

    private void receiveFile() throws ThirdPartyTransferFailedCacheException {
        MoverChannel<RemoteHttpDataTransferProtocolInfo> channel = _channel;
        _transfer.get(_integrityChecker,
              entity -> entity.writeTo(Channels.newOutputStream(channel)),
              progress(),
              channel::truncate);
    }

    private void sendAndCheckFile(FileAttributes attributes)
          throws ThirdPartyTransferFailedCacheException {
        MoverChannel<RemoteHttpDataTransferProtocolInfo> channel = _channel;
        _transfer.putAndVerify(() -> new RepositoryChannelEntity(channel), progress(),
              attributes.getSize(), attributes.getChecksumsIfPresent());
    }

    @Override
    public long getLastTransferred() {
        MoverChannel<RemoteHttpDataTransferProtocolInfo> channel = _channel;
        return channel == null ? System.currentTimeMillis() : channel.getLastTransferred();
    }

    @Override
    public long getBytesTransferred() {
        MoverChannel<RemoteHttpDataTransferProtocolInfo> channel = _channel;
        return channel == null ? 0 : channel.getBytesTransferred();
    }

    @Override
    public long getTransferTime() {
        MoverChannel<RemoteHttpDataTransferProtocolInfo> channel = _channel;
        return channel == null ? 0 : channel.getTransferTime();
    }

    @Override
    public List<InetSocketAddress> remoteConnections() {
        RemoteHttpTransferClient transfer = _transfer;
        return transfer == null ? List.of() : transfer.remoteAddress().stream().toList();
    }

    @Override
    public Long getBytesExpected() {
        RemoteHttpTransferClient transfer = _transfer;
        return transfer == null ? null : transfer.getBytesExpected();
    }

    private void startFlowMarker() {
        if (_startMarkerSent || _localEndpoint == null || _subject == null) {
            return;
        }

        ProtocolInfo protocolInfo = _channel.getProtocolInfo();
        if (!(protocolInfo instanceof IpProtocolInfo ipInfo)) {
            return;
        }

        InetSocketAddress remoteEndpoint = ipInfo.getSocketAddress();
        if (remoteEndpoint == null) {
            return;
        }

        _transferLifeCycle.onStart(remoteEndpoint, _localEndpoint,
              protocolInfo, _subject);
        _startMarkerSent = true;
    }

    @Override
    public Optional<InetSocketAddress> getLocalEndpoint() {
        return Optional.ofNullable(_localEndpoint);
    }
}
