/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 Deutsches Elektronen-Synchrotron
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

import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.FileNotFoundException;
import java.io.InterruptedIOException;
import java.nio.channels.CompletionHandler;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellPath;

import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.classic.PostTransferService;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.TryCatchTemplate;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Abstract base class for movers.
 */
public abstract class AbstractMover<P extends ProtocolInfo, M extends Mover<P>> implements Mover<P>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMover.class);

    private final TypeToken<P> type = new TypeToken<P>(getClass()){};

    protected final long _id;
    protected final String _queue;
    protected final String _initiator;
    protected final boolean _isPoolToPoolTransfer;
    protected final CellPath _pathToDoor;
    protected final P _protocolInfo;
    protected final Subject _subject;
    protected final ReplicaDescriptor _handle;
    protected final IoMode _ioMode;
    protected final TransferService<Mover<P>> _transferService;
    protected final PostTransferService _postTransferService;
    protected final FsPath _path;
    protected volatile int _errorCode;
    protected volatile String _errorMessage = "";

    public AbstractMover(ReplicaDescriptor handle, PoolIoFileMessage message, CellPath pathToDoor,
                         TransferService<M> transferService,
                         PostTransferService postTransferService)
    {
        checkArgument(type.isAssignableFrom(message.getProtocolInfo().getClass()));
        _queue = message.getIoQueueName();
        _protocolInfo = (P) message.getProtocolInfo();
        _initiator = message.getInitiator();
        _isPoolToPoolTransfer = message.isPool2Pool();
        _ioMode = (message instanceof PoolAcceptFileMessage) ? IoMode.WRITE : IoMode.READ;
        _subject = message.getSubject();
        _id = message.getId();
        _path = message.getPnfsPath();
        _pathToDoor = pathToDoor;
        _handle = handle;
        _transferService = (TransferService<Mover<P>>) transferService;
        _postTransferService = postTransferService;
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
            _errorMessage = errorMessage;
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
    public FsPath getPath()
    {
        return _path;
    }

    @Override
    public Subject getSubject()
    {
        return _subject;
    }

    @Override
    public void postprocess(CompletionHandler<Void, Void> completionHandler)
    {
        _postTransferService.execute(this, completionHandler);
    }

    @Override
    public Cancellable execute(CompletionHandler<Void, Void> completionHandler)
    {
        return new TryCatchTemplate<Void, Void>(completionHandler) {
            @Override
            public Cancellable executeWithCancellable()
                    throws Exception
            {
                return _transferService.execute(AbstractMover.this, this);
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
                    setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
                } catch (Exception e) {
                    LOGGER.error("Transfer failed: {}", e.toString());
                    setTransferStatus(CacheException.DEFAULT_ERROR_CODE, e.getMessage());
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
    public RepositoryChannel openChannel() throws DiskErrorCacheException
    {
        RepositoryChannel channel;
        switch (getIoMode()) {
        case WRITE:
            try {
                channel = new FileRepositoryChannel(_handle.getFile(), "rw");
            } catch (FileNotFoundException e) {
                throw new DiskErrorCacheException(
                        "File could not be created; please check the file system", e);
            }
            break;
        case READ:
            try {
                channel = new FileRepositoryChannel(_handle.getFile(), "r");
            } catch (FileNotFoundException e) {
                throw new DiskErrorCacheException("File could not be opened  [" +
                        e.getMessage() + "]; please check the file system", e);
            }
            break;
        default:
            throw new RuntimeException("Invalid I/O mode");
        }
        return channel;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(getFileAttributes().getPnfsId());
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

    protected abstract String getStatus();
}
