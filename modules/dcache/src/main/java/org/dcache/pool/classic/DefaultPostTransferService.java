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
package org.dcache.pool.classic;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.MoverInfoMessage;

import dmg.cells.nucleus.AbstractCellComponent;

import org.dcache.cells.CellStub;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.util.FireAndForgetTask;
import org.dcache.vehicles.FileAttributes;

public class DefaultPostTransferService extends AbstractCellComponent implements PostTransferService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPostTransferService.class);

    private final ExecutorService _executor =
            new CDCExecutorServiceDecorator<>(
                    Executors.newCachedThreadPool(
                            new ThreadFactoryBuilder().setNameFormat("post-transfer-%d").build()));
    private CellStub _billing;
    private String _poolName;
    private FaultListener _faultListener;
    private ChecksumModule _checksumModule;
    private CellStub _door;

    @Required
    public void setBillingStub(CellStub billing) {
        _billing = billing;
    }

    @Required
    public void setPoolName(String poolName) {
        _poolName = poolName;
    }

    @Required
    public void setFaultListener(FaultListener faultListener) {
        _faultListener = faultListener;
    }

    @Required
    public void setChecksumModule(ChecksumModule checksumModule) {
        _checksumModule = checksumModule;
    }

    public void init() {
        _door = new CellStub(getCellEndpoint());
    }

    @Override
    public void execute(final Mover<?> mover, final CompletionHandler<Void, Void> completionHandler)
    {
        _executor.execute(new FireAndForgetTask(new Runnable()
        {
            @Override
            public void run()
            {
                long fileSize = 0;
                try {
                    ReplicaDescriptor handle = mover.getIoHandle();
                    try {
                        fileSize = handle.getFile().length();
                        if (mover.getIoMode() == IoMode.WRITE) {
                            handle.addChecksums(mover.getExpectedChecksums());
                            _checksumModule.enforcePostTransferPolicy(handle, mover.getActualChecksums());
                        }
                        handle.commit();
                    } finally {
                        handle.close();
                    }
                    completionHandler.completed(null, null);
                } catch (InterruptedIOException | InterruptedException e) {
                    LOGGER.warn("Transfer was forcefully killed during post-processing");
                    mover.setTransferStatus(CacheException.DEFAULT_ERROR_CODE,
                            "Transfer was forcefully killed");
                    completionHandler.failed(e, null);
                } catch (DiskErrorCacheException e) {
                    LOGGER.warn("Transfer failed in post-processing due to disk error: {}", e.toString());
                    _faultListener.faultOccurred(
                            new FaultEvent("repository", FaultAction.DISABLED, e.getMessage(), e));
                    mover.setTransferStatus(e.getRc(), "Disk error: " + e.getMessage());
                    completionHandler.failed(e, null);
                } catch (CacheException e) {
                    LOGGER.warn("Transfer failed in post-processing: {}", e.getMessage());
                    mover.setTransferStatus(e.getRc(), "Post-processing failed: " + e.getMessage());
                    completionHandler.failed(e, null);
                } catch (IOException e) {
                    LOGGER.warn("Transfer failed in post-processing: {}", e.toString());
                    mover.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                            "Transfer failed in post-processing: " + e.getMessage());
                    completionHandler.failed(e, null);
                } catch (RuntimeException e) {
                    LOGGER.error(
                            "Transfer failed in post-processing. Please report this bug to support@dcache.org.", e);
                    mover.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                            "Transfer failed due to unexpected exception: " + e.getMessage());
                    completionHandler.failed(e, null);
                } catch (Throwable e) {
                    Thread t = Thread.currentThread();
                    t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    mover.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                            "Transfer failed due to unexpected exception: " + e.getMessage());
                    completionHandler.failed(e, null);
                }
                sendBillingMessage(mover, fileSize);
                sendFinished(mover);
            }
        }));
    }

    public void sendBillingMessage(Mover<?> mover, long fileSize) {
        FileAttributes fileAttributes = mover.getFileAttributes();

        MoverInfoMessage info = new MoverInfoMessage(getCellName(), fileAttributes.getPnfsId());
        info.setSubject(mover.getSubject());
        info.setInitiator(mover.getInitiator());
        info.setFileCreated(mover.getIoMode() == IoMode.WRITE);
        info.setStorageInfo(fileAttributes.getStorageInfo());
        info.setP2P(mover.isPoolToPoolTransfer());
        info.setFileSize(fileSize);
        info.setResult(mover.getErrorCode(), mover.getErrorMessage());
        info.setTransferAttributes(mover.getBytesTransferred(),
                                   mover.getTransferTime(),
                                   mover.getProtocolInfo());
        info.setBillingPath(mover.getBillingPath());
        info.setTransferPath(mover.getTransferPath());

        _billing.notify(info);
    }

    public void sendFinished(Mover<?> mover) {
        DoorTransferFinishedMessage finished =
                new DoorTransferFinishedMessage(mover.getClientId(),
                        mover.getFileAttributes().getPnfsId(),
                        mover.getProtocolInfo(),
                        mover.getFileAttributes(),
                        _poolName,
                        mover.getQueueName());
        if (mover.getErrorCode() == 0) {
            finished.setSucceeded();
        } else {
            finished.setReply(mover.getErrorCode(), mover.getErrorMessage());
        }

        _door.notify(mover.getPathToDoor(), finished);
    }

    public void shutdown()
    {
        _executor.shutdown();
    }
}
