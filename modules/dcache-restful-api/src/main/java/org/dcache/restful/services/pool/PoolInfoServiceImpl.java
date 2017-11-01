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
package org.dcache.restful.services.pool;

import org.springframework.beans.factory.annotation.Required;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.pools.json.PoolCostData;
import diskCacheV111.pools.json.PoolSpaceData;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.util.command.Command;
import org.dcache.cells.json.CellData;
import org.dcache.pool.json.PoolData;
import org.dcache.pool.json.PoolDataDetails;
import org.dcache.pool.json.PoolInfoWrapper;
import org.dcache.pool.movers.json.MoverData;
import org.dcache.pool.nearline.json.NearlineData;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.pool.PoolGroupInfo;
import org.dcache.restful.providers.pool.PoolInfo;
import org.dcache.restful.util.admin.ReadWriteData;
import org.dcache.restful.util.pool.PoolDataRequestProcessor;
import org.dcache.restful.util.pool.PoolDiagnosticInfoCollector;
import org.dcache.services.collector.CellDataCollectingService;
import org.dcache.util.collector.pools.PoolInfoCollectorUtils;
import org.dcache.util.collector.ListenableFutureWrapper;
import org.dcache.util.histograms.Histogram;
import org.dcache.util.histograms.HistogramModel;
import org.dcache.vehicles.pool.CacheEntryInfoMessage;
import org.dcache.vehicles.pool.PoolDataRequestMessage;
import org.dcache.vehicles.pool.PoolFlushListingMessage;
import org.dcache.vehicles.pool.PoolMoverListingMessage;
import org.dcache.vehicles.pool.PoolP2PListingMessage;
import org.dcache.vehicles.pool.PoolRemoveListingMessage;
import org.dcache.vehicles.pool.PoolStageListingMessage;

/**
 * <p>Responsible for serving up data from the cache.</p>
 */
public class PoolInfoServiceImpl extends
                CellDataCollectingService<Map<String, ListenableFutureWrapper<PoolDataRequestMessage>>,
                                PoolDiagnosticInfoCollector>
                implements PoolInfoService, CellMessageReceiver {
    @Command(name = "pools set timeout",
                    hint = "Set the timeout interval between refreshes",
                    description = "Changes the interval between "
                                    + "collections of pool information")
    class PoolsSetTimeoutCommand extends SetTimeoutCommand {
    }

    @Command(name = "pools refresh",
                    hint = "Query for current pool info",
                    description = "Interrupts current wait to run query "
                                    + "immediately.")
    class PoolsRefreshCommand extends RefreshCommand {
        @Override
        public String call() {
            processor.cancel();
            return super.call();
        }
    }

    private final ReadWriteData<String, PoolInfoWrapper> cache
                    = new ReadWriteData<>(true);

    /**
     * <p>Remote monitor, from context.</p>
     */
    private PoolMonitor monitor;

    /**
     * <p>Does the brunt of the updating work on the data returned
     * by the collector.</p>
     */
    private PoolDataRequestProcessor processor;

    /**
     * <p>Throttle on the size of the mover and nearline queue lists
     * that can be returned.</p>
     */
    private int maxPoolActivityListSize;

    public ReadWriteData<String, PoolInfoWrapper> getCache() {
        return cache;
    }

    /**
     * <p>Synchronous.</p>
     */
    @Override
    public void getCacheInfo(String pool, PnfsId pnfsid, PoolInfo info) {
        CacheEntryInfoMessage message = new CacheEntryInfoMessage(pnfsid);
        ListenableFutureWrapper<CacheEntryInfoMessage> wrapper
                        = collector.sendRequestToPool(pool, message);
        try {
            message = wrapper.getFuture().get();
            info.setPnfsidInfo(message.getInfo());
            info.setRepositoryListing(message.getRepositoryListing());
        } catch (InterruptedException e) {
            LOGGER.trace("get cache info interrupted.");
        } catch (ExecutionException e) {
            LOGGER.error("Problem retrieving cache info for {} on {}: "
                                         + "{}, cause: {}.",
                         pnfsid, pool, e.getMessage(), e.getCause());
        }
    }

    /**
     * <p>Delivers cached/local data.</p>
     */
    @Override
    public void getDiagnosticInfo(String name, PoolInfo info) {
        PoolInfoWrapper cached = cache.read(name);
        if (cached != null) {
            PoolData data = cached.getInfo();
            synchronized (this) {
                PoolSelectionUnit psu = getSelectionUnit();
                List<String> groups = psu.getPoolGroupsOfPool(name)
                                         .stream()
                                         .map(SelectionPoolGroup::getName)
                                         .collect(Collectors.toList());
                Set<String> links = groups.stream()
                                          .map(psu::getLinksPointingToPoolGroup)
                                          .flatMap(c -> c.stream())
                                          .map(SelectionLink::getName)
                                          .collect(Collectors.toSet());
                data.setPoolGroups(groups);
                data.setLinks(links);
                info.setPoolData(data);
            }
        }
    }

    /**
     * <p>Delivers cached/local data.</p>
     */
    @Override
    public void getFileStat(String name, PoolInfo info) {
        PoolInfoWrapper cached = cache.read(name);
        if (cached != null) {
            Histogram[] fstat = new Histogram[] {
                            toHistogram(cached.getInfo().getSweeperData()
                                              .getLastAccessHistogram()),
                            toHistogram(cached.getFileLiftimeMax()),
                            toHistogram(cached.getFileLiftimeAvg()),
                            toHistogram(cached.getFileLiftimeMin()),
                            toHistogram(cached.getFileLiftimeStddev()) };
            info.setFileStat(fstat);
        }
    }

    /**
     * <p>Delivers cached/local data.</p>
     */
    @Override
    public void getFileStat(String name, PoolGroupInfo info) {
        PoolInfoWrapper cached = cache.read(name);
        if (cached != null) {
            Histogram[] fstat = new Histogram[] {
                            toHistogram(cached.getInfo().getSweeperData()
                                              .getLastAccessHistogram()),
                            toHistogram(cached.getFileLiftimeMax()),
                            toHistogram(cached.getFileLiftimeAvg()),
                            toHistogram(cached.getFileLiftimeMin()),
                            toHistogram(cached.getFileLiftimeStddev()) };
            info.setGroupFileStat(fstat);
        }
    }

    /**
     * <p>Synchronous.  Delivers fresh data.</p>
     */
    @Override
    public void getFlush(String name, PoolInfo info) {
        PoolFlushListingMessage message = new PoolFlushListingMessage();
        message.setLimit(maxPoolActivityListSize);
        ListenableFutureWrapper<PoolFlushListingMessage> wrapper
                        = collector.sendRequestToPool(name, message);
        try {
            message = wrapper.getFuture().get();
            info.setFlush(message.getData().toArray(new NearlineData[0]));
        } catch (InterruptedException e) {
            LOGGER.trace("get flush listings interrupted.");
        } catch (ExecutionException e) {
            LOGGER.error("Problem retrieving flush listings for {}: {}, cause: {}.",
                         name, e.getMessage(), e.getCause());
        }
    }

    /**
     * <p>Delivers cached/local data.</p>
     */
    @Override
    public void getGroupCellInfos(String name, PoolGroupInfo info) {
        String[] pools = listPools(name);

        Map<String, CellData> data = new HashMap<>();

        for (String pool : pools) {
            data.put(pool, getPoolCellData(pool));
        }

        info.setCellDataForPools(data);
    }

    /**
     * <p>Delivers cached/local data.</p>
     */
    @Override
    public void getGroupQueueInfos(String name, PoolGroupInfo info) {
        if (info.getCostDataForPools() != null) {
            return;
        }

        String[] pools = listPools(name);

        Map<String, PoolCostData> data = new HashMap<>();

        for (String pool : pools) {
            data.put(pool, getPoolCostData(pool));
        }

        info.setCostDataForPools(data);
    }

    /**
     * <p>Delivers cached/local data.</p>
     */
    @Override
    public void getGroupSpaceInfos(String name, PoolGroupInfo info) {
        getGroupQueueInfos(name, info);
        info.setGroupSpaceData(getPoolSpaceData(name));
    }

    /**
     * <p>Synchronous.  Delivers fresh data.</p>
     */
    @Override
    public void getMovers(String name, PoolInfo info) {
        PoolMoverListingMessage message = new PoolMoverListingMessage();
        message.setLimit(maxPoolActivityListSize);
        ListenableFutureWrapper<PoolMoverListingMessage> wrapper
                        = collector.sendRequestToPool(name, message);
        try {
            message = wrapper.getFuture().get();
            info.setMovers(message.getData().toArray(new MoverData[0]));
        } catch (InterruptedException e) {
            LOGGER.trace("get mover listings interrupted.");
        } catch (ExecutionException e) {
            LOGGER.error("Problem retrieving mover listings for {}: {}, cause: {}.",
                         name, e.getMessage(), e.getCause());
        }
    }

    /**
     * <p>Synchronous.  Delivers fresh data.</p>
     */
    @Override
    public void getP2p(String name, PoolInfo info) {
        PoolP2PListingMessage message = new PoolP2PListingMessage();
        message.setLimit(maxPoolActivityListSize);
        ListenableFutureWrapper<PoolP2PListingMessage> wrapper
                        = collector.sendRequestToPool(name, message);
        try {
            message = wrapper.getFuture().get();
            info.setP2ps(message.getData().toArray(new MoverData[0]));
        } catch (InterruptedException e) {
            LOGGER.trace("get P2p listings interrupted.");
        } catch (ExecutionException e) {
            LOGGER.error("Problem retrieving P2p listings for {}: {}, cause: {}.",
                         name, e.getMessage(), e.getCause());
        }
    }

    /**
     * <p>Delivers cached/local data.</p>
     */
    @Override
    public void getQueueStat(String name, PoolInfo info) {
        PoolInfoWrapper cached = cache.read(name);
        if (cached != null) {
            Histogram[] qstat = new Histogram[] {
                            toHistogram(cached.getActiveMovers()),
                            toHistogram(cached.getQueuedMovers()),
                            toHistogram(cached.getActiveP2PClient()),
                            toHistogram(cached.getQueuedP2PClient()),
                            toHistogram(cached.getActiveP2P()),
                            toHistogram(cached.getQueuedP2P()),
                            toHistogram(cached.getActiveFlush()),
                            toHistogram(cached.getQueuedFlush()),
                            toHistogram(cached.getActiveStage()),
                            toHistogram(cached.getQueuedStage()) };
            info.setQueueStat(qstat);
        }
    }

    /**
     * <p>Delivers cached/local data.</p>
     */
    @Override
    public void getQueueStat(String name, PoolGroupInfo info) {
        PoolInfoWrapper cached = cache.read(name);
        if (cached != null) {
            Histogram[] qstat = new Histogram[] {
                            toHistogram(cached.getActiveMovers()),
                            toHistogram(cached.getQueuedMovers()),
                            toHistogram(cached.getActiveP2PClient()),
                            toHistogram(cached.getQueuedP2PClient()),
                            toHistogram(cached.getActiveP2P()),
                            toHistogram(cached.getQueuedP2P()),
                            toHistogram(cached.getActiveFlush()),
                            toHistogram(cached.getQueuedFlush()),
                            toHistogram(cached.getActiveStage()),
                            toHistogram(cached.getQueuedStage()) };
            info.setGroupQueueStat(qstat);
        }
    }

    /**
     * <p>Synchronous.  Delivers fresh data.</p>
     */
    @Override
    public void getRemove(String name, PoolInfo info) {
        PoolRemoveListingMessage message = new PoolRemoveListingMessage();
        message.setLimit(maxPoolActivityListSize);
        ListenableFutureWrapper<PoolRemoveListingMessage> wrapper
                        = collector.sendRequestToPool(name, message);
        try {
            message = wrapper.getFuture().get();
            info.setRemove(message.getData().toArray(new NearlineData[0]));
        } catch (InterruptedException e) {
            LOGGER.trace("get remove listings interrupted.");
        } catch (ExecutionException e) {
            LOGGER.error("Problem retrieving remove listings for {}: {}, cause: {}.",
                         name, e.getMessage(), e.getCause());
        }
    }

    public PoolSelectionUnit getSelectionUnit() {
        return monitor.getPoolSelectionUnit();
    }

    /**
     * <p>Synchronous.  Delivers fresh data.</p>
     */
    @Override
    public void getStage(String name, PoolInfo info) {
        PoolStageListingMessage message = new PoolStageListingMessage();
        message.setLimit(maxPoolActivityListSize);
        ListenableFutureWrapper<PoolStageListingMessage> wrapper
                        = collector.sendRequestToPool(name, message);
        try {
            message = wrapper.getFuture().get();
            info.setStage(message.getData().toArray(new NearlineData[0]));
        } catch (InterruptedException e) {
            LOGGER.trace("get stage listings interrupted.");
        } catch (ExecutionException e) {
            LOGGER.error("Problem retrieving stage listings for {}: {}, cause: {}.",
                         name, e.getMessage(), e.getCause());
        }
    }

    @Override
    public String[] listGroups() {
        return PoolInfoCollectorUtils.listGroups(getSelectionUnit());
    }

    @Override
    public String[] listPools() {
        return PoolInfoCollectorUtils.listPools(getSelectionUnit());
    }

    @Override
    public String[] listPools(String group) {
        return PoolInfoCollectorUtils.listPools(group, getSelectionUnit());
    }

    @Required
    public void setMaxPoolActivityListSize(int maxPoolActivityListSize) {
        this.maxPoolActivityListSize = maxPoolActivityListSize;
    }

    @Required
    public void setPoolMonitor(PoolMonitor poolMonitor) {
        this.monitor = poolMonitor;
    }

    @Required
    public void setProcessor(PoolDataRequestProcessor processor) {
        this.processor = processor;
    }

    /**
     * <p>Callback invoked by processor when it has completed
     * the update.</p>
     *
     * @param next updated map data.
     */
    public void updateJsonData(Map<String, PoolInfoWrapper> next) {
        cache.clearAndWrite(next);
    }

    @Override
    protected void update(
                    Map<String, ListenableFutureWrapper<PoolDataRequestMessage>> data) {
        try {
            processor.process(data);
        } catch (IllegalStateException e) {
            LOGGER.info("Processing cycle for processor has overlapped; you may wish to "
                                        + "increase the interval between pool "
                                        + "info collections, which is currently "
                                        + "set to {} {}.",
                        timeout, timeoutUnit);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Processing failed for the current cycle: {}.",
                        e.getMessage());
        }
    }

    private CellData getPoolCellData(String key) {
        PoolData poolData = getPoolData(key);
        return poolData == null ? null : poolData.getCellData();
    }

    private PoolCostData getPoolCostData(String key) {
        PoolDataDetails details = getPoolDataDetails(key);
        return details == null ? null : details.getCostData();
    }

    private PoolData getPoolData(String key) {
        PoolInfoWrapper cached = cache.read(key);
        return cached == null ? null : cached.getInfo();
    }

    private PoolDataDetails getPoolDataDetails(String key) {
        PoolData poolData = getPoolData(key);
        return poolData == null ? null : poolData.getDetailsData();
    }

    private PoolSpaceData getPoolSpaceData(String key) {
        PoolCostData costData = getPoolCostData(key);
        return costData == null ? null : costData.getSpace();
    }

    /**
     * Avoids NPE if field is null.
     */
    private Histogram toHistogram(HistogramModel model) {
        if (model == null) {
            return null;
        }
        return model.toHistogram();
    }
}
