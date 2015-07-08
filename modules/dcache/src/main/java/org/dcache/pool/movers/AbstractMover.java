/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 - 2015 Deutsches Elektronen-Synchrotron
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

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.InterruptedIOException;
import java.io.IOException;
import java.nio.channels.CompletionHandler;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellPath;

import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.TryCatchTemplate;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Abstract base class for movers.
 */
public abstract class AbstractMover<P extends ProtocolInfo, M extends AbstractMover<P, M>> implements Mover<P>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMover.class);

    protected final long _id;
    protected final String _queue;
    protected final String _initiator;
    protected final boolean _isPoolToPoolTransfer;
    protected final CellPath _pathToDoor;
    protected final P _protocolInfo;
    protected final Subject _subject;
    protected final ReplicaDescriptor _handle;
    protected final IoMode _ioMode;
    protected final TransferService<M> _transferService;
    protected final String _billingPath;
    protected final String _transferPath;
    protected volatile int _errorCode;
    protected volatile String _errorMessage = "";
    private final ChecksumFactory _checksumFactory;
    private volatile ChecksumChannel _checksumChannel;

    public AbstractMover(ReplicaDescriptor handle, PoolIoFileMessage message, CellPath pathToDoor,
                         TransferService<M> transferService,
                         ChecksumModule checksumModule)
    {
        TypeToken<M> type = new TypeToken<M>(getClass()) {};
        checkArgument(type.isAssignableFrom(getClass()));

        _queue = message.getIoQueueName();
        _protocolInfo = (P) message.getProtocolInfo();
        _initiator = message.getInitiator();
        _isPoolToPoolTransfer = message.isPool2Pool();
        _ioMode = (message instanceof PoolAcceptFileMessage) ? IoMode.WRITE : IoMode.READ;
        _subject = message.getSubject();
        _id = message.getId();
        _billingPath = message.getBillingPath();
        _transferPath = message.getTransferPath();
        _pathToDoor = pathToDoor;
        _handle = handle;
        _transferService = transferService;
        _checksumFactory = getChecksumFactoryFor(checksumModule, handle);
    }

    @Override
    public FileAttributes getFileAttributes()
    {
        return _handle.getFileAttributes();
    }

    @Override
    public P getProtocolInfo()
    {
        return _protocolInfo;
    }

    @Override
    public long getClientId()
    {
        return _id;
    }

    @Override
    public void setTransferStatus(int errorCode, String errorMessage)
    {
        if (_errorCode == 0) {
            _errorCode = errorCode;
            _errorMessage = Strings.nullToEmpty(errorMessage);
        }
    }

    @Override
    public String getQueueName()
    {
        return _queue;
    }

    @Override
    public int getErrorCode()
    {
        return _errorCode;
    }

    @Override
    public String getErrorMessage()
    {
        return _errorMessage;
    }

    @Override
    public String getInitiator()
    {
        return _initiator;
    }

    @Override
    public boolean isPoolToPoolTransfer()
    {
        return _isPoolToPoolTransfer;
    }

    @Override
    public ReplicaDescriptor getIoHandle()
    {
        return _handle;
    }

    @Override
    public IoMode getIoMode()
    {
        return _ioMode;
    }

    @Override
    public CellPath getPathToDoor()
    {
        return _pathToDoor;
    }

    @Override
    public String getBillingPath()
    {
        return _billingPath;
    }

    @Override
    public String getTransferPath()
    {
        return _transferPath;
    }

    @Override
    public Subject getSubject()
    {
        return _subject;
    }

    @Override @SuppressWarnings("unchecked")
    public void close(CompletionHandler<Void, Void> completionHandler)
    {
        _transferService.closeMover((M) this, completionHandler);
    }

    @Override
    public Cancellable execute(CompletionHandler<Void, Void> completionHandler)
    {
        return new TryCatchTemplate<Void, Void>(completionHandler) {
            @Override @SuppressWarnings("unchecked")
            public Cancellable executeWithCancellable()
                    throws Exception
            {
                return _transferService.executeMover((M) AbstractMover.this, this);
            }

            @Override
            public synchronized void onFailure(Throwable t, Void attachment)
            {
                try {
                    throw t;
                } catch (DiskErrorCacheException e) {
                    LOGGER.error("Transfer failed due to a disk error: {}", e.toString());
                    setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
                } catch (CacheException e) {
                    LOGGER.error("Transfer failed: {}", e.getMessage());
                    setTransferStatus(e.getRc(), e.getMessage());
                } catch (InterruptedIOException | InterruptedException e) {
                    LOGGER.error("Transfer was forcefully killed");
                    setTransferStatus(CacheException.DEFAULT_ERROR_CODE, "Transfer was forcefully killed");
                } catch (RuntimeException e) {
                    LOGGER.error("Transfer failed due to a bug", e);
                    setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, "Bug detected (please report): " + e.getMessage());
                } catch (Exception e) {
                    LOGGER.error("Transfer failed: {}", e.toString());
                    setTransferStatus(CacheException.DEFAULT_ERROR_CODE, "General problem: " + e.getMessage());
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
     *
     * The caller is responsible for closing the stream at the end of the transfer.
     *
     * TODO: Consider moving this method to RepositoryChannel.
     *
     * @return An open RepositoryChannel to the replica of this mover
     * @throws DiskErrorCacheException If the file could not be opened
     */
    public RepositoryChannel openChannel() throws DiskErrorCacheException {
        RepositoryChannel channel;
        try {
            channel = new FileRepositoryChannel(_handle.getFile(), getIoMode().toOpenString());
            if (getIoMode() == IoMode.WRITE) {
                try {
                    channel = _checksumChannel = new ChecksumChannel(channel, _checksumFactory);
                } catch (Throwable t) {
                    /* This should only happen in case of JVM Errors or if the checksum digest cannot be
                     * instantiated (which, barring bugs, should never happen).
                     */
                    try {
                        channel.close();
                    } catch (IOException e) {
                        t.addSuppressed(e);
                    }
                    Throwables.propagate(t);
                }
            }
        } catch (IOException e) {
            throw new DiskErrorCacheException(
                    "File could not be opened; please check the file system: " + e.getMessage(), e);
        }
        return channel;
    }

    @Override
    public Set<Checksum> getActualChecksums() {
        return (_checksumChannel == null)
                ? Collections.<Checksum>emptySet()
                : Optional.fromNullable(_checksumChannel.getChecksum()).asSet();
    }

    @Override
    public Set<Checksum> getExpectedChecksums() {
        return Collections.emptySet();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(getFileAttributes().getPnfsId());
        sb.append(" IoMode=").append(getIoMode());
        sb.append(" h={").append(getStatus()).append("} bytes=").append(getBytesTransferred()).append(
                " time/sec=").append(getTransferTime() / 1000L).append(" LM=");
        long lastTransferTime = getLastTransferred();
        if (lastTransferTime == 0L) {
            sb.append(0);
        } else {
            sb.append((System.currentTimeMillis() - lastTransferTime) / 1000L);
        }
        return sb.toString();
    }

    private static ChecksumFactory getChecksumFactoryFor(ChecksumModule checksumModule, ReplicaDescriptor handle)
    {
        try {
            return checksumModule.getPreferredChecksumFactory(handle);
        } catch (NoSuchAlgorithmException | CacheException e) {
            LOGGER.error("Failed to instantiate mover due to unsupported checksum type: " + e.getMessage(), e);
        }
        return null;
    }

    protected abstract String getStatus();
}
