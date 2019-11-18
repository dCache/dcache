/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.resilience.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.resilience.data.FileUpdate;
import org.dcache.resilience.data.MessageType;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolStateUpdate;
import org.dcache.resilience.util.BrokenFileTask;
import org.dcache.resilience.util.ExceptionMessage;
import org.dcache.resilience.util.MessageGuard;
import org.dcache.resilience.util.MessageGuard.Status;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.vehicles.CorruptFileMessage;
import org.dcache.vehicles.PnfsSetFileAttributes;

/**
 * <p>Processes external messages in order to trigger file operations.
 *      Also handles internal callbacks indicated the change of
 *      pool status/mode.</p>
 */
public final class ResilienceMessageHandler implements CellMessageReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(
                    ResilienceMessageHandler.class);
    private static final Logger ACTIVITY_LOGGER =
                    LoggerFactory.getLogger("org.dcache.resilience-log");

    private MessageGuard         messageGuard;
    private FileOperationHandler fileOperationHandler;
    private PoolOperationHandler poolOperationHandler;
    private PoolInfoMap          poolInfoMap;
    private OperationStatistics  counters;
    private ExecutorService      updateService;

    /**
     * <p>Pool status changes are detected internally during the
     *      comparison of pool monitor states.  The change is rerouted
     *      here as a loopback to conform with the way external file updates
     *      are handled.</p>
     *
     * <p>Pools that have not yet been fully initialized are skipped.</p>
     */
    public void handleInternalMessage(PoolStateUpdate update) {
        if (!messageGuard.isEnabled()) {
            LOGGER.trace("Ignoring pool state update "
                            + "because message guard is disabled.");
            return;
        }

        updateService.submit(() -> {
            poolInfoMap.updatePoolStatus(update);
            if (poolInfoMap.isInitialized(update.pool)) {
                counters.incrementMessage(update.getStatus().getMessageType().name());
                poolOperationHandler.handlePoolStatusChange(update);
            }
        });
    }

    public void messageArrived(CorruptFileMessage message) {
        ACTIVITY_LOGGER.info("Received notice that file {} on pool {} is corrupt.",
                             message.getPnfsId(), message.getPool());
        if (messageGuard.getStatus("CorruptFileMessage", message)
                        == Status.DISABLED) {
            return;
        }
        handleBrokenFile(message);
    }

    public void messageArrived(PnfsAddCacheLocationMessage message) {
        ACTIVITY_LOGGER.info("Received notice that pool {} received file {}.",
                             message.getPoolName(), message.getPnfsId());
        if (messageGuard.getStatus("PnfsAddCacheLocationMessage", message)
                        != Status.EXTERNAL) {
            return;
        }
        handleAddCacheLocation(message);
    }

    public void messageArrived(PnfsClearCacheLocationMessage message) {
        ACTIVITY_LOGGER.info("Received notice that pool {} cleared file {}.",
                             message.getPoolName(), message.getPnfsId());
        if (messageGuard.getStatus("PnfsClearCacheLocationMessage", message)
                        != Status.EXTERNAL) {
            return;
        }
        handleClearCacheLocation(message);
    }

    public void messageArrived(PoolMigrationCopyFinishedMessage message) {
        ACTIVITY_LOGGER.info("Received notice that transfer {} of file "
                                             + "{} from {} has finished.",
                             message.getUUID(), message.getPnfsId(), message.getPool());
        fileOperationHandler.handleMigrationCopyFinished(message);
    }

    public void messageArrived(CellMessage message, PoolMgrSelectReadPoolMsg reply) {
        ACTIVITY_LOGGER.info("Received notice that file {} has been staged to pool {}",
                             reply.getPool(),
                             reply.getPnfsId());
        if (messageGuard.getStatus("PoolMgrSelectReadPoolMsg", message)
                        == Status.DISABLED) {
            return;
        }
        handleStagingRetry(reply);
    }

    public void messageArrived(PnfsSetFileAttributes message) {
        ACTIVITY_LOGGER.info("Received notice that qos for file {} has changed.",
                             message.getPnfsId());
        if (messageGuard.getStatus("FileQoSMessage", message)
                        == Status.DISABLED) {
            return;
        }
        handleQoSModification(message);
    }

    public void processBackloggedMessage(Message message) {
        if (message instanceof CorruptFileMessage) {
            handleBrokenFile((CorruptFileMessage) message);
        } else if (message instanceof PnfsClearCacheLocationMessage) {
            handleClearCacheLocation((PnfsClearCacheLocationMessage) message);
        } else if (message instanceof PnfsAddCacheLocationMessage) {
            handleAddCacheLocation((PnfsAddCacheLocationMessage) message);
        } else if (message instanceof PoolMgrSelectReadPoolMsg) {
            handleStagingRetry((PoolMgrSelectReadPoolMsg)message);
        } else if (message instanceof PnfsSetFileAttributes) {
            handleQoSModification((PnfsSetFileAttributes)message);
        }
    }

    public void setCounters(OperationStatistics counters) {
        this.counters = counters;
    }

    public void setMessageGuard(MessageGuard messageGuard) {
        this.messageGuard = messageGuard;
    }

    public void setFileOperationHandler(
                    FileOperationHandler fileOperationHandler) {
        this.fileOperationHandler = fileOperationHandler;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolOperationHandler(PoolOperationHandler poolOperationHandler) {
        this.poolOperationHandler = poolOperationHandler;
    }

    public void setUpdateService(ExecutorService updateService) {
        this.updateService = updateService;
    }

    private void handleAddCacheLocation(PnfsAddCacheLocationMessage message) {
        counters.incrementMessage(MessageType.ADD_CACHE_LOCATION.name());
        updatePnfsLocation(new FileUpdate(message.getPnfsId(), message.getPoolName(),
                                          MessageType.ADD_CACHE_LOCATION, true));
    }

    private void handleBrokenFile(CorruptFileMessage message) {
        counters.incrementMessage(MessageType.CORRUPT_FILE.name());
        new BrokenFileTask(message.getPnfsId(), message.getPool(),
                           fileOperationHandler).submit();
    }

    private void handleClearCacheLocation(PnfsClearCacheLocationMessage message) {
        counters.incrementMessage(MessageType.CLEAR_CACHE_LOCATION.name());
        updatePnfsLocation(new FileUpdate(message.getPnfsId(),
                                          message.getPoolName(),
                                          MessageType.CLEAR_CACHE_LOCATION,
                                          false));
    }

    private void handleQoSModification(PnfsSetFileAttributes message) {
        counters.incrementMessage(MessageType.QOS_MODIFIED.name());
        /*
         * It is actually unnecessary to inspect the AL/RP at this point,
         * as we will refresh during processing.
         */
        updatePnfsLocation(new FileUpdate(message.getPnfsId(), null,
                                          MessageType.QOS_MODIFIED, true));
    }

    private void handleStagingRetry(PoolMgrSelectReadPoolMsg reply) {
        updateService.submit(() -> {
                fileOperationHandler.handleStagingReply(reply);
        });
    }

    private void updatePnfsLocation(FileUpdate data) {
        updateService.submit(() -> {
            try {
                fileOperationHandler.handleLocationUpdate(data);
            } catch (CacheException e) {
                LOGGER.error("Error in verification of location update data {}: {}",
                             data, new ExceptionMessage(e));
            }
        });
    }
}
