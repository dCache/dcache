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
package org.dcache.pool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import diskCacheV111.pools.json.PoolCostData;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.Reply;

import org.dcache.cells.MessageReply;
import org.dcache.cells.json.CellData;
import org.dcache.pool.classic.IoQueueManager;
import org.dcache.pool.classic.PoolV4;
import org.dcache.pool.classic.json.ChecksumModuleData;
import org.dcache.pool.classic.json.FlushControllerData;
import org.dcache.pool.classic.json.HSMFlushQManagerData;
import org.dcache.pool.classic.json.JobTimeoutManagerData;
import org.dcache.pool.classic.json.SweeperData;
import org.dcache.pool.classic.json.TransferServicesData;
import org.dcache.pool.json.PoolData;
import org.dcache.pool.migration.json.MigrationData;
import org.dcache.pool.movers.json.MoverData;
import org.dcache.pool.nearline.NearlineStorageHandler;
import org.dcache.pool.p2p.json.P2PData;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.json.RepositoryData;
import org.dcache.vehicles.pool.CacheEntryInfoMessage;
import org.dcache.vehicles.pool.PoolDataRequestMessage;
import org.dcache.vehicles.pool.PoolFlushListingMessage;
import org.dcache.vehicles.pool.PoolLiveDataForHistoriesMessage;
import org.dcache.vehicles.pool.PoolMoverListingMessage;
import org.dcache.vehicles.pool.PoolP2PListingMessage;
import org.dcache.vehicles.pool.PoolRemoveListingMessage;
import org.dcache.vehicles.pool.PoolStageListingMessage;

/**
 * <p>Serves requests from frontend pool info service for info relating to the
 * mover, flush, stage and remove listings, as well as repository
 * cache info for a particular pnfsid.<p>
 *
 * <p>The full diagnostic information concerning the pool is obtained using
 *    the {@link PoolDataRequestMessage}.</p>
 */
public final class PoolInfoRequestHandler implements CellMessageReceiver,
                CellInfoAware {
    private PoolDataBeanProvider<ChecksumModuleData>    checksumModule;
    private PoolDataBeanProvider<FlushControllerData>   flushController;
    private PoolDataBeanProvider<HSMFlushQManagerData>  hsmFlushQueueManager;
    private PoolDataBeanProvider<JobTimeoutManagerData> jobTimeoutManager;
    private PoolDataBeanProvider<MigrationData>         migrationClient;
    private PoolDataBeanProvider<MigrationData>         migrationServer;
    private PoolDataBeanProvider<P2PData>               p2pClient;
    private PoolDataBeanProvider<RepositoryData>        repositoryProvider;
    private PoolDataBeanProvider<SweeperData>           sweeper;
    private PoolDataBeanProvider<TransferServicesData>  transferServices;
    private PoolV4                                      pool;
    private IoQueueManager                              queueManager;
    private Repository                                  repository;
    private NearlineStorageHandler storageHandler;
    private ExecutorService        executor;
    private Supplier<CellInfo>     supplier;

    /**
     * <p>Gathers diagnostic and detail information about various
     * cell components for the pool, including sweeper lifetime
     * values and migration jobs.</p>
     */
    public Reply messageArrived(PoolDataRequestMessage message) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                PoolData request = new PoolData();

                request.setCellData(getCellInfoRequest());
                request.setDetailsData(pool.getDataObject());
                request.setCsmData(checksumModule.getDataObject());
                request.setFlushData(flushController.getDataObject());
                request.setHsmFlushQMData(
                                hsmFlushQueueManager.getDataObject());
                request.setJtmData(jobTimeoutManager.getDataObject());
                MigrationData client = migrationClient.getDataObject();
                MigrationData service = migrationServer.getDataObject();
                client.setServerRequests(service.getServerRequests());
                request.setMigrationData(client);
                request.setPpData(p2pClient.getDataObject());
                request.setRepositoryData(repositoryProvider.getDataObject());
                request.setStorageHandlerData(storageHandler.getDataObject());
                request.setSweeperData(sweeper.getDataObject());
                request.setTransferServicesData(transferServices.getDataObject());

                message.setData(request);
                reply.reply(message);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(PoolLiveDataForHistoriesMessage message) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                message.setPoolCostData(new PoolCostData(pool.getPoolCostInfo()));
                message.setSweeperData(sweeper.getDataObject());
                reply.reply(message);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
        return reply;
    }

    /**
     * <p>Gets cache information from the repository for a given pnfsid.</p>
     */
    public Reply messageArrived(CacheEntryInfoMessage message) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                PnfsId pnfsId = message.getPnfsId();
                message.setRepositoryListing(repository.getEntry(pnfsId)
                                                       .toString());
                message.setInfo(pool.getCacheRepositoryEntryInfo(
                                message.getPnfsId()));
                reply.reply(message);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
        return reply;
    }

    /**
     * <p>Gathers queue listings corresponding to mover ls,
     * but excludes the p2p client queue.</p>
     */
    public Reply messageArrived(PoolMoverListingMessage info) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                List<MoverData> data = new ArrayList<>();
                queueManager.queues()
                            .stream()
                            .filter((q) -> !"p2p".equalsIgnoreCase(q.getName()))
                            .flatMap((q) -> q.getMoverData(info.getLimit()).stream())
                            .forEach((d) -> {
                                data.add(d);
                            });
                info.setData(data);
                reply.reply(info);
            } catch (Exception e) {
                reply.fail(info, e);
            }
        });
        return reply;
    }

    /**
     * <p>Gathers queue listings corresponding to mover ls for p2p clients,
     * and the p2p server queue.</p>
     */
    public Reply messageArrived(PoolP2PListingMessage info) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                List<MoverData> data = new ArrayList<>();
                int limit = info.getLimit();
                queueManager.queues()
                            .stream()
                            .filter((q) -> "p2p".equalsIgnoreCase(q.getName()))
                            .flatMap((q) -> q.getMoverData(limit).stream())
                            .forEach((d) -> {
                                d.setQueue("CLIENT");
                                data.add(d);
                            });
                queueManager.getPoolToPoolQueue().getMoverData(limit)
                            .stream()
                            .forEach((d) -> {
                                d.setQueue("SERVER");
                                data.add(d);
                            });
                info.setData(data);
                reply.reply(info);
            } catch (Exception e) {
                reply.fail(info, e);
            }
        });
        return reply;
    }

    /**
     * <p>Gathers queue listings corresponding to st ls.</p>
     */
    public Reply messageArrived(PoolFlushListingMessage info) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                info.setData(storageHandler.getFlushRequests(info.getLimit()));
                reply.reply(info);
            } catch (Exception e) {
                reply.fail(info, e);
            }
        });
        return reply;
    }

    /**
     * <p>Gathers queue listings corresponding to rh ls.</p>
     */
    public Reply messageArrived(PoolStageListingMessage info) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                info.setData(storageHandler.getStageRequests(info.getLimit()));
                reply.reply(info);
            } catch (Exception e) {
                reply.fail(info, e);
            }
        });
        return reply;
    }

    /**
     * <p>Gathers queue listings corresponding to rm ls.</p>
     */
    public Reply messageArrived(PoolRemoveListingMessage info) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                info.setData(storageHandler.getRemoveRequests(info.getLimit()));
                reply.reply(info);
            } catch (Exception e) {
                reply.fail(info, e);
            }
        });
        return reply;
    }

    @Override
    public void setCellInfoSupplier(Supplier<CellInfo> supplier) {
        this.supplier = supplier;
    }

    public void setChecksumModule(PoolDataBeanProvider checksumModule) {
        this.checksumModule = checksumModule;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public void setFlushController(
                    PoolDataBeanProvider flushController) {
        this.flushController = flushController;
    }

    public void setHsmFlushQueueManager(
                    PoolDataBeanProvider hsmFlushQueueManager) {
        this.hsmFlushQueueManager = hsmFlushQueueManager;
    }

    public void setJobTimeoutManager(
                    PoolDataBeanProvider jobTimeoutManager) {
        this.jobTimeoutManager = jobTimeoutManager;
    }

    public void setMigrationClient(
                    PoolDataBeanProvider migrationClient) {
        this.migrationClient = migrationClient;
    }

    public void setMigrationServer(
                    PoolDataBeanProvider migrationServer) {
        this.migrationServer = migrationServer;
    }

    public void setP2pClient(PoolDataBeanProvider p2pClient) {
        this.p2pClient = p2pClient;
    }

    public void setPool(PoolV4 pool) {
        this.pool = pool;
    }

    public void setQueueManager(IoQueueManager queueManager) {
        this.queueManager = queueManager;
    }

    public void setRepository(PoolDataBeanProvider repository) {
        this.repositoryProvider = repository;
        this.repository = (Repository)repository;
    }

    public void setStorageHandler(
                    NearlineStorageHandler storageHandler) {
        this.storageHandler = storageHandler;
    }

    public void setSweeper(PoolDataBeanProvider sweeper) {
        this.sweeper = sweeper;
    }

    public void setTransferServices(
                    PoolDataBeanProvider transferServices) {
        this.transferServices = transferServices;
    }

    private CellData getCellInfoRequest() {
        CellData request = new CellData();
        CellInfo info = supplier.get();
        request.setCreationTime(info.getCreationTime());
        request.setDomainName(info.getDomainName());
        request.setCellType(info.getCellType());
        request.setCellName(info.getCellName());
        request.setCellClass(info.getCellClass());
        request.setEventQueueSize(info.getEventQueueSize());
        request.setExpectedQueueTime(info.getExpectedQueueTime());
        request.setLabel("Cell Info");
        CellVersion version = info.getCellVersion();
        request.setRelease(version.getRelease());
        request.setRevision(version.getRevision());
        request.setVersion(version.toString());
        request.setState(info.getState());
        request.setThreadCount(info.getThreadCount());
        return request;
    }
}
