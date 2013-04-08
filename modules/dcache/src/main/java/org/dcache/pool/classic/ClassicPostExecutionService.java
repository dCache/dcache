package org.dcache.pool.classic;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.InterruptedIOException;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import diskCacheV111.util.CacheException;
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
public class ClassicPostExecutionService extends AbstractCellComponent implements PostTransferExecutionService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClassicPostExecutionService.class);

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
    public void execute(final PoolIORequest request, final CompletionHandler<Void,Void> completionHandler)
    {
        _executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    try {
                        request.close();
                    } catch (InterruptedException | InterruptedIOException e) {
                        request.setTransferStatus(CacheException.DEFAULT_ERROR_CODE,
                                "Transfer was killed");
                    } catch (CacheException e) {
                        int rc = e.getRc();
                        String msg = e.getMessage();
                        if (rc == CacheException.ERROR_IO_DISK) {
                            _faultListener.faultOccurred(new FaultEvent("repository", FaultAction.DISABLED, msg, e));
                        }
                        request.setTransferStatus(rc, msg);
                    } catch (Exception e) {
                        request.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                "Transfer failed due to unexpected exception: " + e.getMessage());
                    } catch (Throwable e) {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                        request.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                "Transfer failed due to unexpected exception: " + e.getMessage());
                    }
                    sendBillingMessage(request);
                    sendFinished(request);
                } finally {
                    completionHandler.completed(null, null);
                }
            }
        });
    }

    public void sendBillingMessage(PoolIORequest request) {
        PoolIOTransfer transfer = request.getTransfer();
        FileAttributes fileAttributes = transfer.getFileAttributes();

        MoverInfoMessage info = new MoverInfoMessage(getCellName(), fileAttributes.getPnfsId());
        info.setSubject(transfer.getSubject());
        info.setInitiator(request.getInitiator());
        info.setFileCreated(transfer instanceof PoolIOWriteTransfer);
        info.setStorageInfo(fileAttributes.getStorageInfo());
        info.setP2P(request.isPoolToPoolTransfer());
        info.setFileSize(transfer.getFileSize());
        info.setResult(request.getErrorCode(), request.getErrorMessage());
        info.setTransferAttributes(transfer.getBytesTransferred(),
                transfer.getTransferTime(),
                transfer.getProtocolInfo());

        try {
            _billing.send(info);
        } catch (NoRouteToCellException e) {
            LOGGER.error("Failed to register transfer in billing: {}", e.getMessage());
        }
    }

    public void sendFinished(PoolIORequest request) {
        PoolIOTransfer transfer = request.getTransfer();
        DoorTransferFinishedMessage finished =
                new DoorTransferFinishedMessage(request.getClientId(),
                        transfer.getFileAttributes().getPnfsId(),
                        transfer.getProtocolInfo(),
                        transfer.getFileAttributes(),
                        _poolName,
                        request.getQueue());
        if (request.getErrorCode() == 0) {
            finished.setSucceeded();
        } else {
            finished.setReply(request.getErrorCode(), request.getErrorMessage());
        }

        try {
            request.sendToDoor(finished);
        } catch (NoRouteToCellException e) {
            LOGGER.error("Failed to notify door about transfer termination: {}", e.getMessage());
        }
    }

    public void shutdown()
    {
        _executor.shutdown();
    }
}
