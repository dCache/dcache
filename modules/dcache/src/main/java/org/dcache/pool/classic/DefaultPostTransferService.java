/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 - 2023 Deutsches Elektronen-Synchrotron
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

import static org.dcache.util.Exceptions.messageOrClassName;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.MoverInfoMessage;
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellInfoProvider;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.dcache.cells.CellStub;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.TransferLifeCycle;
import org.dcache.pool.repository.ModifiableReplicaDescriptor;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.statistics.DirectedIoStatistics;
import org.dcache.pool.statistics.IoStatistics;
import org.dcache.pool.statistics.IoStatisticsChannel;
import org.dcache.pool.statistics.SnapshotStatistics;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.util.FireAndForgetTask;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;

public class DefaultPostTransferService extends AbstractCellComponent implements
      PostTransferService, CellInfoProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPostTransferService.class);

    private final ExecutorService _executor =
          new CDCExecutorServiceDecorator<>(
                Executors.newCachedThreadPool(
                      new ThreadFactoryBuilder().setNameFormat("post-transfer-%d").build()));
    private CellStub _billing;
    private String _poolName;
    private ChecksumModule _checksumModule;
    private CellStub _door;

    private Consumer<MoverInfoMessage> _kafkaSender = (s) -> {
    };

    private TransferLifeCycle transferLifeCycle;

    @Required
    public void setBillingStub(CellStub billing) {
        _billing = billing;
    }

    @Required
    public void setPoolName(String poolName) {
        _poolName = poolName;
    }

    @Required
    public void setChecksumModule(ChecksumModule checksumModule) {
        _checksumModule = checksumModule;
    }

    @Autowired(required = false)
    @Qualifier("transfer")
    public void setKafkaTemplate(KafkaTemplate kafkaTemplate) {
        _kafkaSender = kafkaTemplate::sendDefault;
    }

    public void setTransferLifeCycle(TransferLifeCycle transferLifeCycle) {
        this.transferLifeCycle = transferLifeCycle;
    }

    public void init() {
        _door = new CellStub(getCellEndpoint());
    }

    @Override
    public void execute(final Mover<?> mover,
          final CompletionHandler<Void, Void> completionHandler) {
        _executor.execute(new FireAndForgetTask(() -> {
            ReplicaDescriptor handle = mover.getIoHandle();
            try {
                try {
                    if (mover.getIoMode().contains(StandardOpenOption.WRITE)) {
                        ModifiableReplicaDescriptor modHandle = (ModifiableReplicaDescriptor)handle;

                        modHandle.addChecksums(mover.getExpectedChecksums());
                        _checksumModule.enforcePostTransferPolicy(modHandle,
                              mover.getActualChecksums());
                        modHandle.commit();
                    }
                } finally {
                    handle.close();
                }
                completionHandler.completed(null, null);
            } catch (InterruptedIOException | InterruptedException e) {
                LOGGER.warn("Transfer was forcefully killed during post-processing");
                mover.setTransferStatus(CacheException.DEFAULT_ERROR_CODE,
                      "Transfer was forcefully killed");
                completionHandler.failed(e, null);
            } catch (CacheException e) {
                LOGGER.warn("Transfer failed in post-processing: {}", e.getMessage());
                mover.setTransferStatus(e.getRc(), "Post-processing failed: " + e.getMessage());
                completionHandler.failed(e, null);
            } catch (IOException e) {
                LOGGER.warn("Transfer failed in post-processing: {}", e.toString());
                mover.setTransferStatus(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                      "Transfer failed in post-processing: " + messageOrClassName(e));
                completionHandler.failed(e, null);
            } catch (RuntimeException e) {
                LOGGER.error(
                      "Transfer failed in post-processing. Please report this bug to support@dcache.org.",
                      e);
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
            MoverInfoMessage moverInfoMessage = generateBillingMessage(mover,
                  handle.getReplicaSize());

            // as current thread is used to serialize the message, send finish to the door before notifying the billing.
            sendFinished(mover, moverInfoMessage);
            sendBillingInfo(moverInfoMessage);
        }));
    }

    private void sendBillingInfo(MoverInfoMessage moverInfoMessage) {
        _billing.notify(moverInfoMessage);

        try {
            _kafkaSender.accept(moverInfoMessage);
        } catch (KafkaException | org.apache.kafka.common.KafkaException e) {
            LOGGER.warn("Failed to send message to kafka: {} ", Throwables.getRootCause(e).getMessage());
        }
    }

    public MoverInfoMessage generateBillingMessage(Mover<?> mover, long fileSize) {

        FileAttributes fileAttributes = mover.getFileAttributes();

        MoverInfoMessage info = new MoverInfoMessage(getCellAddress(), fileAttributes.getPnfsId());

        info.setSubject(mover.getSubject());
        info.setInitiator(mover.getInitiator());
        info.setFileCreated(mover.getIoMode().contains(StandardOpenOption.WRITE));
        info.setStorageInfo(fileAttributes.getStorageInfo());
        info.setP2P(mover.isPoolToPoolTransfer());
        info.setFileSize(fileSize);
        info.setResult(mover.getErrorCode(), mover.getErrorMessage());
        info.setTransferAttributes(mover.getBytesTransferred(),
              mover.getTransferTime(),
              mover.getProtocolInfo());
        info.setBillingPath(mover.getBillingPath());
        info.setTransferPath(mover.getTransferPath());
        mover.getLocalEndpoint().ifPresent(info::setLocalEndpoint);

        MoverInfoMessage infoWithStats = mover.getChannel()
              .flatMap(c -> c.optionallyAs(IoStatisticsChannel.class))
              .map(c -> c.getStatistics())
              .map(s -> updateIoStatistics(info, s))
              .orElse(info);

        return infoWithStats;
    }


    private static MoverInfoMessage updateIoStatistics(MoverInfoMessage info,
          IoStatistics statistics) {
        DirectedIoStatistics reads = statistics.reads();
        DirectedIoStatistics writes = statistics.writes();
        SnapshotStatistics readStats = reads.statistics();
        SnapshotStatistics writeStats = writes.statistics();

        if (readStats.requestedBytes().getN() > 0) {
            info.setMeanReadBandwidth(readStats.instantaneousBandwidth().getMean());
            info.setReadActive(reads.active());
            info.setReadIdle(reads.idle());
        }

        if (writeStats.requestedBytes().getN() > 0) {
            info.setMeanWriteBandwidth(writeStats.instantaneousBandwidth().getMean());
            info.setWriteActive(writes.active());
            info.setWriteIdle(writes.idle());
        }

        return info;
    }

    public void sendFinished(Mover<?> mover, MoverInfoMessage moverInfoMessage) {
        DoorTransferFinishedMessage finished =
              new DoorTransferFinishedMessage(mover.getClientId(),
                    mover.getFileAttributes().getPnfsId(),
                    mover.getProtocolInfo(),
                    mover.getFileAttributes(),
                    _poolName,
                    mover.getQueueName());
        finished.setMoverInfo(moverInfoMessage);
        if (mover.getErrorCode() == 0) {
            finished.setSucceeded();
        } else {
            finished.setReply(mover.getErrorCode(), mover.getErrorMessage());
        }

        mover.getLocalEndpoint().ifPresent(e ->
                transferLifeCycle.onEnd(((IpProtocolInfo) mover.getProtocolInfo()).getSocketAddress(),
                        e,
                        mover.getProtocolInfo(),
                        mover.getSubject()));

        _door.notify(mover.getPathToDoor(), finished);
    }

    public void shutdown() {
        MoreExecutors.shutdownAndAwaitTermination(_executor, 10, TimeUnit.SECONDS);
    }
}
