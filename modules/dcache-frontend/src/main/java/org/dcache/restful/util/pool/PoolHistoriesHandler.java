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
package org.dcache.restful.util.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import diskCacheV111.pools.json.PoolCostData;
import diskCacheV111.pools.json.PoolSpaceData;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.pool.classic.json.SweeperData;
import org.dcache.pool.json.PoolData;
import org.dcache.pool.json.PoolDataDetails;
import org.dcache.pool.json.PoolInfoWrapper;
import org.dcache.restful.services.pool.PoolInfoServiceImpl;
import org.dcache.services.history.pools.PoolTimeseriesService;
import org.dcache.util.collector.pools.PoolInfoAggregator;
import org.dcache.util.collector.pools.PoolInfoCollectorUtils;
import org.dcache.util.histograms.CountingHistogram;
import org.dcache.util.histograms.TimeseriesHistogram;
import org.dcache.vehicles.histograms.AggregateFileLifetimeRequestMessage;
import org.dcache.vehicles.histograms.PoolTimeseriesRequestMessage;
import org.dcache.vehicles.histograms.PoolTimeseriesRequestMessage.TimeseriesType;

import static dmg.cells.nucleus.CellEndpoint.SendFlag.RETRY_ON_NO_ROUTE_TO_CELL;

/**
 * <p>Called during the collection gathering in order to obtain
 * historical (i.e., stateful) pool data such as queue/mover timeseries
 * counts or the running statistics on file lifetime.</p>
 *
 * <p>Delegates to a service interface to obtain the individual pool
 * data objects.</p>
 *
 * <p>Also responsible for aggregation of cost info data according to
 * pool groups.</p>
 */
public final class PoolHistoriesHandler extends PoolInfoAggregator
                implements PoolTimeseriesService {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(PoolHistoriesHandler.class);
    private CellStub            historyService;
    private PoolInfoServiceImpl poolInfoService;

    /**
     * <p>Responsible for adding the timeseries histograms to the
     * info object.</p>
     *
     * <p>Also adds sweeper data.</p>
     *
     * @param info to be updated with most recent historical data.
     * @throws CacheException
     */
    public void addHistoricalData(PoolInfoWrapper info)
                    throws InterruptedException,
                    CacheException, NoRouteToCellException {
        PoolTimeseriesRequestMessage message =
                        getHistogramAndSweeperData(info.getKey(),
                                                   PoolTimeseriesRequestMessage.ALL);
        PoolData poolData = info.getInfo();
        poolData.setSweeperData(message.getSweeperData());

        Map<TimeseriesType, TimeseriesHistogram> timeseries
                        = message.getHistogramMap();

        info.setActiveFlush(timeseries.get(TimeseriesType.ACTIVE_FLUSH));
        info.setActiveMovers(timeseries.get(TimeseriesType.ACTIVE_MOVERS));
        info.setActiveP2P(timeseries.get(TimeseriesType.ACTIVE_P2P));
        info.setActiveP2PClient(
                        timeseries.get(TimeseriesType.ACTIVE_P2P_CLIENT));
        info.setActiveStage(timeseries.get(TimeseriesType.ACTIVE_STAGE));
        info.setQueuedFlush(timeseries.get(TimeseriesType.QUEUED_FLUSH));
        info.setQueuedMovers(timeseries.get(TimeseriesType.QUEUED_MOVERS));
        info.setQueuedP2P(timeseries.get(TimeseriesType.QUEUED_P2P));
        info.setQueuedP2PClient(
                        timeseries.get(TimeseriesType.QUEUED_P2P_CLIENT));
        info.setQueuedStage(timeseries.get(TimeseriesType.QUEUED_STAGE));
        info.setFileLiftimeMax(
                        timeseries.get(TimeseriesType.FILE_LIFETIME_MAX));
        info.setFileLiftimeAvg(
                        timeseries.get(TimeseriesType.FILE_LIFETIME_AVG));
        info.setFileLiftimeMin(
                        timeseries.get(TimeseriesType.FILE_LIFETIME_MIN));
        info.setFileLiftimeStddev(
                        timeseries.get(TimeseriesType.FILE_LIFETIME_STDDEV));
    }

    /*
     *  First try to fetch the stored aggregate histogram.
     *  Failing that, do the aggregation here.
     */
    public CountingHistogram getAggregateFileLifetime(String poolGroup,
                                                      List<PoolInfoWrapper> pools){
        AggregateFileLifetimeRequestMessage message
                        = new AggregateFileLifetimeRequestMessage(poolGroup);

        try {
            message = historyService.sendAndWait(message,
                                                 historyService.getTimeoutInMillis(),
                                                 RETRY_ON_NO_ROUTE_TO_CELL);
        } catch (NoRouteToCellException | InterruptedException | TimeoutCacheException e) {
            LOGGER.debug("Could not fetch aggregated lifetime data for {}: {}.",
                         poolGroup, e.getMessage());
        } catch (CacheException e) {
            LOGGER.warn("Could not fetch aggregated lifetime data for {}: {}.",
                        poolGroup, e.getMessage());
        }

        Serializable error = message.getErrorObject();

        if (error != null) {
            LOGGER.warn("Could not fetch aggregated lifetime data for {}: {}.",
                         message.getPoolGroup(), error);
        }

        CountingHistogram histogram = message.getAggregateLifetime();

        /*
         *  If missing, try to merge here.
         */
        if (histogram == null) {
            LOGGER.info("Fetch of aggregated lifetime histogram unsuccessful; "
                                        + "merging data here.");
            histogram = PoolInfoCollectorUtils.mergeLastAccess(pools);
        }

        return histogram;
    }

    public PoolTimeseriesRequestMessage getHistogramAndSweeperData(String pool,
                                                                   Set<TimeseriesType> types)
                    throws CacheException, InterruptedException,
                    NoRouteToCellException {
        PoolTimeseriesRequestMessage message = new PoolTimeseriesRequestMessage();
        message.setPool(pool);
        message.setKeys(types);
        return historyService.sendAndWait(message,
                                          historyService.getTimeoutInMillis(),
                                          RETRY_ON_NO_ROUTE_TO_CELL);
    }

    @Override
    public Map<TimeseriesType, TimeseriesHistogram> getTimeseries(String pool,
                                                                  Set<TimeseriesType> types)
                    throws CacheException, InterruptedException,
                    NoRouteToCellException {
        return getHistogramAndSweeperData(pool, types).getHistogramMap();
    }

    @Required
    public void setHistoryService(CellStub historyService) {
        this.historyService = historyService;
    }

    @Required
    public void setPoolInfoService(PoolInfoServiceImpl poolInfoService) {
        this.poolInfoService = poolInfoService;
    }

    @Override
    protected PoolInfoWrapper getAggregateWrapper(String key) {
        PoolInfoWrapper groupInfo = poolInfoService.getCache().read(key);
        if (groupInfo == null) {
            groupInfo = new PoolInfoWrapper();
            groupInfo.setKey(key);
        }
        return groupInfo;
    }

    /**
     * @param group stored data for the group
     * @param pools stored data for the pools of the group
     */
    @Override
    protected void update(PoolInfoWrapper group, List<PoolInfoWrapper> pools) {
        PoolCostData groupCost = PoolInfoCollectorUtils.createCostDataIfEmpty(group);
        PoolSpaceData groupSpace = new PoolSpaceData();
        groupCost.setSpace(groupSpace);
        pools.stream()
             .map(PoolInfoWrapper::getInfo)
             .filter(Objects::nonNull)
             .map(PoolData::getDetailsData)
             .filter(Objects::nonNull)
             .map(PoolDataDetails::getCostData)
             .filter(Objects::nonNull)
             .map(PoolCostData::getSpace)
             .filter(Objects::nonNull)
             .forEach(groupSpace::aggregateData);

        PoolData poolData = new PoolData();
        PoolDataDetails details = new PoolDataDetails();
        poolData.setDetailsData(details);
        details.setCostData(groupCost);
        group.setInfo(poolData);

        CountingHistogram model = getAggregateFileLifetime(group.getKey(), pools);
        SweeperData sweeperData = new SweeperData();
        sweeperData.setLastAccessHistogram(model);
        poolData.setSweeperData(sweeperData);

        try {
            addHistoricalData(group);
        } catch (NoRouteToCellException | InterruptedException | TimeoutCacheException e) {
            LOGGER.debug("Could not add historical data for {}: {}.",
                         group, e.getMessage());
        } catch (CacheException e) {
            LOGGER.error("Could not add historical data for {}: {}.",
                         group, e.getMessage());
        }
    }
}
