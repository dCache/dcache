/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 - 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.movers;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.util.Exceptions.messageOrClassName;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.CellPath;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.security.auth.Subject;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.OutOfDiskException;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Exceptions;
import org.dcache.util.TryCatchTemplate;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for movers.
 */
public abstract class AbstractMover<P extends ProtocolInfo, M extends AbstractMover<P, M>> implements
      Mover<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMover.class);

    protected final long _id;
    protected final String _queue;
    protected final String _initiator;
    protected final boolean _isPoolToPoolTransfer;
    protected final CellPath _pathToDoor;
    protected final P _protocolInfo;
    protected final Subject _subject;
    protected final ReplicaDescriptor _handle;
    protected final Set<? extends OpenOption> _ioMode;
    protected final TransferService<M> _transferService;
    protected final String _billingPath;
    protected final String _transferPath;
    protected volatile int _errorCode;
    protected volatile String _errorMessage = "";
    private final Set<ChecksumType> _checksumTypes = EnumSet.noneOf(ChecksumType.class);
    private final Set<Checksum> _checksums = new HashSet<>();
    private volatile ChecksumChannel _checksumChannel;
    private volatile Optional<RepositoryChannel> _channel = Optional.empty();

    public AbstractMover(ReplicaDescriptor handle, PoolIoFileMessage message, CellPath pathToDoor,
          TransferService<M> transferService) {
        TypeToken<M> type = new TypeToken<M>(getClass()) {
        };
        checkArgument(type.isSupertypeOf(getClass()));

        _queue = message.getIoQueueName();
        _protocolInfo = (P) message.getProtocolInfo();
        _initiator = message.getInitiator();
        _isPoolToPoolTransfer = message.isPool2Pool();
        _ioMode = (message instanceof PoolAcceptFileMessage) ? FileStore.O_RW : FileStore.O_READ;
        _subject = message.getSubject();
        _id = message.getId();
        _billingPath = message.getBillingPath();
        _transferPath = message.getTransferPath();
        _pathToDoor = pathToDoor;
        _handle = handle;
        _transferService = transferService;
    }

    @Override
    public void addChecksumType(ChecksumType type) {
        synchronized (_checksumTypes) {
            if (_checksumChannel == null) {
                _checksumTypes.add(type);
            } else {
                try {
                    _checksumChannel.addType(type);
                } catch (IOException e) {
                    LOGGER.warn("Failed to add {} calculation: {}", type.getName(),
                          messageOrClassName(e));
                }
            }
        }
    }

    public void addExpectedChecksum(Checksum checksum) {
        addChecksumType(checksum.getType());
        _checksums.add(checksum);
    }

    @Override
    public Set<Checksum> getExpectedChecksums() {
        return _checksums;
    }

    @Override
    public FileAttributes getFileAttributes() {
        return _handle.getFileAttributes();
    }

    @Override
    public P getProtocolInfo() {
        return _protocolInfo;
    }

    @Override
    public long getClientId() {
        return _id;
    }

    @Override
    public void setTransferStatus(int errorCode, String errorMessage) {
        if (_errorCode == 0) {
            _errorCode = errorCode;
            _errorMessage = Strings.nullToEmpty(errorMessage);
        }
    }

    @Override
    public String getQueueName() {
        return _queue;
    }

    @Override
    public int getErrorCode() {
        return _errorCode;
    }

    @Override
    public String getErrorMessage() {
        return _errorMessage;
    }

    @Override
    public String getInitiator() {
        return _initiator;
    }

    @Override
    public boolean isPoolToPoolTransfer() {
        return _isPoolToPoolTransfer;
    }

    @Override
    public ReplicaDescriptor getIoHandle() {
        return _handle;
    }

    @Override
    public Set<? extends OpenOption> getIoMode() {
        return _ioMode;
    }

    @Override
    public CellPath getPathToDoor() {
        return _pathToDoor;
    }

    @Override
    public String getBillingPath() {
        return _billingPath;
    }

    @Override
    public String getTransferPath() {
        return _transferPath;
    }

    @Override
    public Subject getSubject() {
        return _subject;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void close(CompletionHandler<Void, Void> completionHandler) {
        _transferService.closeMover((M) this, completionHandler);
    }

    @Override
    public Cancellable execute(CompletionHandler<Void, Void> completionHandler) {
        return new TryCatchTemplate<Void, Void>(completionHandler) {
            @Override
            @SuppressWarnings("unchecked")
            public Cancellable executeWithCancellable()
                  throws Exception {
                return _transferService.executeMover((M) AbstractMover.this, this);
            }

            @Override
            public synchronized void onFailure(Throwable t, Void attachment) {
                try {
                    throw t;
                } catch (DiskErrorCacheException e) {
                    LOGGER.error("Transfer failed due to a disk error: {}", e.toString());
                    setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
                } catch (CacheException e) {
                    LOGGER.error("Transfer failed: {}", e.getMessage());
                    setTransferStatus(e.getRc(), e.getMessage());
                } catch (InterruptedIOException | InterruptedException e) {
                    String message = e.getMessage() != null
                          ? ("Transfer forcefully killed: " + e.getMessage())
                          : "Transfer was forcefully killed";
                    LOGGER.error(message);
                    setTransferStatus(CacheException.DEFAULT_ERROR_CODE, message);
                } catch (RuntimeException e) {
                    LOGGER.error("Transfer failed due to a bug", e);
                    setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                          "Bug detected (please report): " + e.getMessage());
                } catch (OutOfDiskException e) {
                    LOGGER.debug("Transfer failed due to insufficient capacity: {}",
                          e.getMessage());
                    setTransferStatus(CacheException.RESOURCE,
                          "Insufficient capacity: " + e.getMessage());
                } catch (Exception e) {
                    LOGGER.error("Transfer failed: {}", e.toString());
                    setTransferStatus(CacheException.DEFAULT_ERROR_CODE,
                          "General problem: " + Exceptions.messageOrClassName(e));
                } catch (Throwable e) {
                    LOGGER.error("Transfer failed:", e);
                    Thread thread = Thread.currentThread();
                    thread.getUncaughtExceptionHandler().uncaughtException(thread, e);
                    setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
                }
            }
        };
    }

    /**
     * Opens a RepositoryChannel for the replica being transferred by this mover.
     * <p>
     * The caller is responsible for closing the stream at the end of the transfer.
     * <p>
     * TODO: Consider moving this method to RepositoryChannel.
     *
     * @return An open RepositoryChannel to the replica of this mover
     * @throws InterruptedIOException  if the mover was cancelled
     * @throws DiskErrorCacheException If the file could not be opened
     */
    public RepositoryChannel openChannel() throws DiskErrorCacheException, InterruptedIOException {
        RepositoryChannel channel;
        try {
            channel = _handle.createChannel();
        } catch (AsynchronousCloseException e) {
            throw new InterruptedIOException(
                  "mover interrupted while opening file: " + messageOrClassName(e));
        } catch (IOException e) {
            throw new DiskErrorCacheException(
                  "File could not be opened; please check the file system: "
                        + messageOrClassName(e), e);
        }

        synchronized (_checksumTypes) {
            _checksumChannel = channel.optionallyAs(ChecksumChannel.class).orElse(null);
            if (_checksumChannel != null) {
                try {
                    for (Checksum c : _handle.getChecksums()) {
                        try {
                            _checksumChannel.addType(c.getType());
                        } catch (IOException e) {
                            LOGGER.error(
                                  "On-the-fly {} calculation (for known checksum) not possible: {}",
                                  c.getType(), messageOrClassName(e));
                        }
                    }
                } catch (CacheException e) {
                    LOGGER.warn("Failed to fetch checksum information: {}", e.getMessage());
                }

                for (ChecksumType type : _checksumTypes) {
                    try {
                        _checksumChannel.addType(type);
                    } catch (IOException e) {
                        LOGGER.error(
                              "On-the-fly {} calculation (for protocol-delivered checksum) not possible: {}",
                              type, messageOrClassName(e));
                    }
                }
            }
        }

        if (!_channel.isPresent()) {
            _channel = Optional.of(channel);
        }
        return channel;
    }

    @Override
    public Optional<RepositoryChannel> getChannel() {
        return _channel;
    }

    @Nonnull
    @Override
    public Set<Checksum> getActualChecksums() {
        return (_checksumChannel == null)
              ? Collections.emptySet()
              : _checksumChannel.getChecksums();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getFileAttributes().getPnfsId());
        sb.append(" IoMode=").append(getIoMode());
        sb.append(" h={").append(getStatus()).append("} bytes=").append(getBytesTransferred())
              .append(
                    " time/sec=").append(getTransferTime() / 1000L).append(" LM=");
        long lastTransferTime = getLastTransferred();
        if (lastTransferTime == 0L) {
            sb.append(0);
        } else {
            sb.append((System.currentTimeMillis() - lastTransferTime) / 1000L);
        }
        return sb.toString();
    }

    protected abstract String getStatus();
}
