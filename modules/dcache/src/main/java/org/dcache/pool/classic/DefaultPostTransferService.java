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

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellStub;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.pool.FaultListener;
import org.dcache.vehicles.FileAttributes;

/**
 *
 * @since 1.9.11
 */
public class DefaultPostTransferService extends AbstractCellComponent implements PostTransferService<PoolIOTransfer>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPostTransferService.class);

    private final ExecutorService _executor =
            new CDCExecutorServiceDecorator(
                    Executors.newCachedThreadPool(
                            new ThreadFactoryBuilder().setNameFormat("post-execution-%d").build()));
    private CellStub _billing;
    private String _poolName;
    private FaultListener _faultListener;

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

    @Override
    public void execute(final PoolIOTransfer transfer, final CompletionHandler<Void,Void> completionHandler)
    {
        _executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    try {
                        transfer.close();
                    } catch (InterruptedIOException | InterruptedException e) {
                        LOGGER.warn("Transfer was forcefully killed during post-processing");
                        transfer.setTransferStatus(CacheException.DEFAULT_ERROR_CODE,
                                "Transfer was forcefully killed");
                    } catch (DiskErrorCacheException e) {
                        LOGGER.warn("Transfer failed in post-processing due to disk error: {}", e.toString());
                        _faultListener.faultOccurred(new FaultEvent("repository", FaultAction.DISABLED, e.getMessage(), e));
                        transfer.setTransferStatus(e.getRc(), e.getMessage());
                    } catch (CacheException e) {
                        LOGGER.warn("Transfer failed in post-processing: {}", e.getMessage());
                        transfer.setTransferStatus(e.getRc(), e.getMessage());
                    } catch (IOException e) {
                        LOGGER.warn("Transfer failed in post-processing: {}", e.toString());
                        transfer.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                "Transfer failed in post-processing: " + e.getMessage());
                    } catch (RuntimeException e) {
                        LOGGER.error("Transfer failed in post-processing due to unexpected exception", e);
                        transfer.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                "Transfer failed due to unexpected exception: " + e.getMessage());
                    } catch (Throwable e) {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                        transfer.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                "Transfer failed due to unexpected exception: " + e.getMessage());
                    }
                    sendBillingMessage(transfer);
                    sendFinished(transfer);
                } finally {
                    completionHandler.completed(null, null);
                }
            }
        });
    }

    public void sendBillingMessage(PoolIOTransfer transfer) {
        FileAttributes fileAttributes = transfer.getFileAttributes();

        MoverInfoMessage info = new MoverInfoMessage(getCellName(), fileAttributes.getPnfsId());
        info.setSubject(transfer.getSubject());
        info.setInitiator(transfer.getInitiator());
        info.setFileCreated(transfer instanceof PoolIOWriteTransfer);
        info.setStorageInfo(fileAttributes.getStorageInfo());
        info.setP2P(transfer.isPoolToPoolTransfer());
        info.setFileSize(transfer.getFileSize());
        info.setResult(transfer.getErrorCode(), transfer.getErrorMessage());
        info.setTransferAttributes(transfer.getBytesTransferred(),
                transfer.getTransferTime(),
                transfer.getProtocolInfo());

        try {
            _billing.send(info);
        } catch (NoRouteToCellException e) {
            LOGGER.error("Failed to register transfer in billing: {}", e.getMessage());
        }
    }

    public void sendFinished(PoolIOTransfer transfer) {
        DoorTransferFinishedMessage finished =
                new DoorTransferFinishedMessage(transfer.getClientId(),
                        transfer.getFileAttributes().getPnfsId(),
                        transfer.getProtocolInfo(),
                        transfer.getFileAttributes(),
                        _poolName,
                        transfer.getQueueName());
        if (transfer.getErrorCode() == 0) {
            finished.setSucceeded();
        } else {
            finished.setReply(transfer.getErrorCode(), transfer.getErrorMessage());
        }

        try {
            transfer.sendToDoor(finished);
        } catch (NoRouteToCellException e) {
            LOGGER.error("Failed to notify door about transfer termination: {}", e.getMessage());
        }
    }

    public void shutdown()
    {
        _executor.shutdown();
    }
}
