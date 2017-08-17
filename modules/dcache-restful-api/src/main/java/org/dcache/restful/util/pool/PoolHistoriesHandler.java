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

import org.apache.commons.math3.util.FastMath;
import org.springframework.beans.factory.annotation.Required;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.pools.json.PoolCostData;
import diskCacheV111.pools.json.PoolSpaceData;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.NoRouteToCellException;
import org.dcache.cells.CellStub;
import org.dcache.pool.classic.json.SweeperData;
import org.dcache.pool.json.PoolData;
import org.dcache.pool.json.PoolDataDetails;
import org.dcache.pool.json.PoolInfoWrapper;
import org.dcache.restful.services.pool.PoolInfoService;
import org.dcache.services.history.pools.PoolTimeseriesService;
import org.dcache.util.collector.pools.PoolInfoCollectorUtils;
import org.dcache.util.histograms.CountingHistogram;
import org.dcache.util.histograms.HistogramMetadata;
import org.dcache.util.histograms.TimeseriesHistogram;
import org.dcache.vehicles.histograms.PoolTimeseriesRequestMessage;
import org.dcache.vehicles.histograms.PoolTimeseriesRequestMessage.TimeseriesType;

import static dmg.cells.nucleus.CellEndpoint.SendFlag.RETRY_ON_NO_ROUTE_TO_CELL;

/**
 * <p>Called during the collection gathering in order to obtain
 * historical (i.e., stateful) pool data such as queue/mover timeseries
 * counts or the running statistics on file lifetime.</p>
 * <p>
 * <p>Delegates to a service interface to obtain the individual pool
 * data objects.</p>
 * <p>
 * <p>Also responsible for aggregation of historical data according to
 * pool groups.</p>
 */
public final class PoolHistoriesHandler implements PoolTimeseriesService {
    /**
     * <p>Merges the data for the pools into the aggregate space info and
     * time series histograms for the pool group.</p>
     *
     * @param data current map of historical timeseries data from pools.
     * @param psu  for determining pools of pool groups.
     * @throws CacheException
     */
    public static void aggregateDataForPoolGroups(
                    Map<String, PoolInfoWrapper> data,
                    PoolSelectionUnit psu)
                    throws CacheException {
        psu.getPoolGroups()
           .values().stream()
           .map(SelectionPoolGroup::getName)
           .forEach((group) -> {
               PoolInfoWrapper groupInfo = new PoolInfoWrapper();
               List<PoolInfoWrapper> poolInfo
                               = fetch(psu.getPoolsByPoolGroup(group), data);
               update(groupInfo, poolInfo);
               data.put(group, groupInfo);
           });

        /*
         *  Aggregate for all pools.
         */
        PoolInfoWrapper groupInfo = new PoolInfoWrapper();
        List<PoolInfoWrapper> poolInfo
                        = fetch(psu.getAllDefinedPools(false), data);
        update(groupInfo, poolInfo);
        data.put(PoolInfoService.ALL, groupInfo);
    }

    /**
     * <p>Creates empty placeholders in order to aggregate pool space
     * data (used for pool group info).</p>
     *
     * @param info the pool group data
     * @return the existing or new cost data object
     */
    private static PoolCostData createCostDataIfEmpty(PoolInfoWrapper info) {
        PoolData poolData = info.getInfo();
        if (poolData == null) {
            poolData = new PoolData();
            info.setInfo(poolData);
        }

        PoolDataDetails detailsData = poolData.getDetailsData();
        if (detailsData == null) {
            detailsData = new PoolDataDetails();
            poolData.setDetailsData(detailsData);
        }

        PoolCostData groupCostData = detailsData.getCostData();
        if (groupCostData == null) {
            groupCostData = new PoolCostData();
            detailsData.setCostData(groupCostData);
        }

        return groupCostData;
    }

    /**
     * @param pools of the group
     * @return list of current pool info objects
     */
    private static List<PoolInfoWrapper> fetch(
                    Collection<SelectionPool> pools,
                    Map<String, PoolInfoWrapper> data) {
        return pools.stream()
                    .map(SelectionPool::getName)
                    .map(data::get)
                    .filter((e) -> e != null)
                    .collect(Collectors.toList());
    }

    /**
     * <p>Combines the pool last access data into an aggregate for the group.</p>
     *
     * @param allHistograms of the pools of the group
     * @return aggregated histogram model
     */
    private static CountingHistogram mergeLastAccess(
                    List<CountingHistogram> allHistograms) {
        CountingHistogram groupHistogram = new CountingHistogram();
        HistogramMetadata metadata = new HistogramMetadata();

        if (allHistograms.isEmpty()) {
            groupHistogram.setMetadata(metadata);
            return groupHistogram;
        }

        /*
         *  Find the histogram with the highest last bin (and consequently
         *  the widest bins).
         *
         *  Merge the statistics.
         */
        double maxBinValue = Double.MIN_VALUE;
        CountingHistogram standard = null;
        for (CountingHistogram h : allHistograms) {
            double currentMaxBin = h.getHighestBin();
            if (currentMaxBin > maxBinValue) {
                standard = h;
                maxBinValue = currentMaxBin;
            }
            metadata.mergeStatistics(h.getMetadata());
        }

        int binCount = standard.getBinCount();
        double binSize = standard.getBinSize();

        groupHistogram.setBinCount(binCount);
        groupHistogram.setBinSize(binSize);
        groupHistogram.setBinUnit(standard.getBinUnit());
        groupHistogram.setBinUnitLabel(standard.getBinUnitLabel());
        groupHistogram.setBinWidth(standard.getBinWidth());
        groupHistogram.setDataUnitLabel(standard.getDataUnitLabel());
        groupHistogram.setHighestBin(standard.getHighestBin());
        groupHistogram.setLowestBin(standard.getLowestBin());
        groupHistogram.setIdentifier(standard.getIdentifier());
        groupHistogram.setMetadata(metadata);

        /*
         *  Configuration of counting histogram assumes raw unordered
         *  data.  To merge counting histograms, we just need to sum the
         *  already configured data to the correct bin.
         */
        double[] dataArray = new double[binCount];
        for (CountingHistogram h : allHistograms) {
            List<Double> currentData = h.getData();
            double currentBinSize = h.getBinSize();
            int numBins = currentData.size();
            for (int bin = 0; bin < numBins; ++bin) {
                int groupBin = (int) FastMath.floor(
                                (bin * currentBinSize) / binSize);
                dataArray[groupBin] += currentData.get(bin);
            }
        }

        List<Double> groupData = new ArrayList<>();
        for (double d : dataArray) {
            groupData.add(d);
        }

        groupHistogram.setData(groupData);

        return groupHistogram;
    }

    /**
     * @param group stored data for the group
     * @param pools stored data for the pools of the group
     */
    private static void update(PoolInfoWrapper group,
                               List<PoolInfoWrapper> pools) {
        long timestamp = System.currentTimeMillis();

        PoolCostData groupCost = createCostDataIfEmpty(group);
        PoolSpaceData groupSpace = new PoolSpaceData();
        groupCost.setSpace(groupSpace);
        pools.stream()
             .map(PoolInfoWrapper::getInfo)
             .map(PoolData::getDetailsData)
             .map(PoolDataDetails::getCostData)
             .map(PoolCostData::getSpace)
             .filter((s) -> s != null)
             .forEach(groupSpace::aggregateData);

        List<CountingHistogram> allHistograms =
                        pools.stream()
                             .map(PoolInfoWrapper::getInfo)
                             .map(PoolData::getSweeperData)
                             .map(SweeperData::getLastAccessHistogram)
                             .collect(Collectors.toList());

        CountingHistogram model = mergeLastAccess(allHistograms);

        PoolData poolData = new PoolData();
        PoolDataDetails details = new PoolDataDetails();
        poolData.setDetailsData(details);
        details.setCostData(groupCost);
        SweeperData sweeperData = new SweeperData();
        sweeperData.setLastAccessHistogram(model);
        poolData.setSweeperData(sweeperData);
        group.setInfo(poolData);

        PoolInfoCollectorUtils.updateFstatTimeSeries(model.getMetadata(), group,
                                                     timestamp);
        PoolInfoCollectorUtils.updateQstatTimeSeries(pools, group, timestamp);
    }

    private CellStub historyService;

    /**
     * <p>Responsible for adding the timeseries histograms to the
     * info object.</p>
     *
     * @param info to be updated with most recent historical data.
     * @throws CacheException
     */
    public void addHistoricalData(PoolInfoWrapper info)
                    throws InterruptedException,
                    CacheException, NoRouteToCellException {

        Map<TimeseriesType, TimeseriesHistogram> timeseries
                        = getTimeseries(info.getKey(),
                                        PoolTimeseriesRequestMessage.ALL);

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

    @Override
    public Map<TimeseriesType, TimeseriesHistogram> getTimeseries(String pool,
                                                                  Set<TimeseriesType> types)
                    throws CacheException, InterruptedException,
                    NoRouteToCellException {
        PoolTimeseriesRequestMessage message = new PoolTimeseriesRequestMessage();
        message.setPool(pool);
        message.setKeys(types);
        message = historyService.sendAndWait(message,
                                             historyService.getTimeoutInMillis(),
                                             RETRY_ON_NO_ROUTE_TO_CELL);
        return message.getHistogramMap();
    }

    @Required
    public void setHistoryService(CellStub historyService) {
        this.historyService = historyService;
    }
}
