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
package org.dcache.util.collector.pools;

import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.pools.json.PoolCostData;
import diskCacheV111.pools.json.PoolQueueData;

import org.dcache.pool.classic.json.SweeperData;
import org.dcache.pool.json.PoolData;
import org.dcache.pool.json.PoolDataDetails;
import org.dcache.pool.json.PoolInfoWrapper;
import org.dcache.util.histograms.CountingHistogram;
import org.dcache.util.histograms.HistogramMetadata;
import org.dcache.util.histograms.TimeFrame;
import org.dcache.util.histograms.TimeFrame.BinType;
import org.dcache.util.histograms.TimeseriesHistogram;

import static org.dcache.services.history.pools.PoolListingService.ALL;

/**
 * <p>Utility class aiding in the extraction and updating of
 * information relevant to pools.  These mainly have to do
 * with updating and aggregating histogram data.</p>
 */
public final class PoolInfoCollectorUtils {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(PoolInfoCollectorUtils.class);

    /**
     * <p>Placeholder used internally for updating and aggregating
     * queue request counts for pool groups.</p>
     */
    static class QueueData {
        Double activeMovers;
        Double queuedMovers;
        Double activeP2P;
        Double queuedP2P;
        Double activeP2PClient;
        Double queuedP2PClient;
        Double activeFlush;
        Double queuedFlush;
        Double activeStage;
        Double queuedStage;

        QueueData(PoolCostData costData) {
            if (costData == null) {
                return;
            }

            PoolQueueData queueData = costData.getMover();
            if (queueData != null) {
                activeMovers = (double) queueData.getActive();
                queuedMovers = (double) queueData.getQueued();
            }

            queueData = costData.getP2p();
            if (queueData != null) {
                activeP2P = (double) queueData.getActive();
                queuedP2P = (double) queueData.getQueued();
            }

            queueData = costData.getP2pClient();
            if (queueData != null) {
                activeP2PClient = (double) queueData.getActive();
                queuedP2PClient = (double) queueData.getQueued();
            }

            queueData = costData.getStore();
            if (queueData != null) {
                activeFlush = (double) queueData.getActive();
                queuedFlush = (double) queueData.getQueued();
            }

            queueData = costData.getRestore();
            if (queueData != null) {
                activeStage = (double) queueData.getActive();
                queuedStage = (double) queueData.getQueued();
            }
        }

        QueueData(List<PoolInfoWrapper> poolInfo) {
            activeMovers = 0.0D;
            queuedMovers = 0.0D;
            activeP2P = 0.0D;
            queuedP2P = 0.0D;
            activeP2PClient = 0.0D;
            queuedP2PClient = 0.0D;
            activeFlush = 0.0D;
            queuedFlush = 0.0D;
            activeStage = 0.0D;
            queuedStage = 0.0D;

            if (poolInfo != null) {
                poolInfo.stream()
                        .map(PoolInfoWrapper::getInfo)
                        .map(PoolData::getDetailsData)
                        .map(PoolDataDetails::getCostData)
                        .forEach(this::increment);
            }
        }

        private void increment(PoolCostData costData) {
            if (costData == null) {
                return;
            }

            PoolQueueData queueData = costData.getMover();
            if (queueData != null) {
                activeMovers += (double) queueData.getActive();
                queuedMovers += (double) queueData.getQueued();
            }

            queueData = costData.getP2p();
            if (queueData != null) {
                activeP2P += (double) queueData.getActive();
                queuedP2P += (double) queueData.getQueued();
            }

            queueData = costData.getP2pClient();
            if (queueData != null) {
                activeP2PClient += (double) queueData.getActive();
                queuedP2PClient += (double) queueData.getQueued();
            }

            queueData = costData.getStore();
            if (queueData != null) {
                activeFlush += (double) queueData.getActive();
                queuedFlush += (double) queueData.getQueued();
            }

            queueData = costData.getRestore();
            if (queueData != null) {
                activeStage += (double) queueData.getActive();
                queuedStage += (double) queueData.getQueued();
            }
        }
    }

    /**
     * <p>Creates empty placeholders in order to aggregate pool space
     * data (used for pool group info).</p>
     *
     * @param info the pool group data
     * @return the existing or new cost data object
     */
    public static PoolCostData createCostDataIfEmpty(PoolInfoWrapper info) {
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
     * <p>Accesses current state of selection unit.</p>
     */
    public static String[] listGroups(PoolSelectionUnit poolSelectionUnit) {
        if (poolSelectionUnit == null) {
            return new String[0];
        }

        return poolSelectionUnit.getPoolGroups().values()
                                .stream()
                                .sorted(Comparator.comparing(SelectionPoolGroup::getName))
                                .map(SelectionPoolGroup::getName)
                                .toArray(String[]::new);
    }

    /**
     * <p>Accesses current state of selection unit.</p>
     */
    public static String[] listPools(PoolSelectionUnit poolSelectionUnit) {
        if (poolSelectionUnit == null) {
            return new String[0];
        }

        return poolSelectionUnit.getPools().values()
                                .stream()
                                .sorted(Comparator.comparing(SelectionPool::getName))
                                .map(SelectionPool::getName)
                                .toArray(String[]::new);
    }

    /**
     * <p>Accesses current state of selection unit.</p>
     */
    public static String[] listPools(String group, PoolSelectionUnit poolSelectionUnit) {
        if (group == null || ALL.equalsIgnoreCase(group)) {
            return listPools(poolSelectionUnit);
        }

        if (poolSelectionUnit == null) {
            return new String[0];
        }

        return poolSelectionUnit.getPoolsByPoolGroup(group)
                                .stream()
                                .sorted(Comparator.comparing(SelectionPool::getName))
                                .map(SelectionPool::getName)
                                .toArray(String[]::new);
    }

    /**
     * <p>Combines the pool last access data into an aggregate for the group.</p>
     *
     * @param pools the pools of the group
     * @return aggregated histogram model
     */
    public static CountingHistogram mergeLastAccess(List<PoolInfoWrapper> pools) {
        List<CountingHistogram> allHistograms =
                        pools.stream()
                             .map(PoolInfoWrapper::getInfo)
                             .filter(Objects::nonNull)
                             .map(PoolData::getSweeperData)
                             .filter(Objects::nonNull)
                             .map(SweeperData::getLastAccessHistogram)
                             .filter(Objects::nonNull)
                             .collect(Collectors.toList());

        CountingHistogram groupHistogram
                        = SweeperData.createLastAccessHistogram();

        if (allHistograms.isEmpty()) {
            groupHistogram.setData(Collections.EMPTY_LIST);
            groupHistogram.configure();
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
        HistogramMetadata metadata = new HistogramMetadata();

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
        groupHistogram.setBinWidth(standard.getBinWidth());
        groupHistogram.setHighestBin(standard.getHighestBin());
        groupHistogram.setLowestBin(standard.getLowestBin());
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
     * <p>Generates a histogram model which tracks
     * the average, maximum, minimum or standard deviation of lifetime
     * values over a fixed window of two months.</p>
     *
     * <p>An empty list of values is used to initialize the bins.</p>
     *
     * @param type       MAX, MIN, AVG, STDDEV
     * @param identifier for the histogram
     * @return the corresponding histogram model
     */
    public static TimeseriesHistogram newLifetimeTimeSeriesHistogram(
                    String type,
                    String identifier) {
        double highestBin = (double) TimeFrame.computeHighTimeFromNow(
                        BinType.DAY)
                                              .getTimeInMillis();
        double unit = (double) TimeUnit.DAYS.toMillis(1);
        TimeseriesHistogram histogram = new TimeseriesHistogram();
        histogram.setData(Collections.EMPTY_LIST);
        histogram.setBinUnit(unit);
        histogram.setBinUnitLabel("DATE");
        histogram.setDataUnitLabel(type);
        histogram.setBinCount(61);
        histogram.setHighestBin(highestBin);
        histogram.setIdentifier(identifier);
        histogram.configure();
        return histogram;
    }

    /**
     * <p>Generates a histogram model which tracks
     * the request count for active or queued movers, stores, restores or
     * p2ps over a fixed window of 48 hours.  Time intervals of 5
     * minutes are used for the bins.</p>
     *
     * <p>An empty list of values is used to initialize the bins.</p>
     *
     * @param identifier for the histogram
     * @return the corresponding histogram model
     */
    public static TimeseriesHistogram newQueueTimeSeriesHistogram(
                    String identifier) {
        long now = System.currentTimeMillis();
        long modulo = TimeUnit.MINUTES.toMillis(5);
        double highestBin = now - (now % modulo);

        TimeseriesHistogram histogram = new TimeseriesHistogram();
        histogram.setData(Collections.EMPTY_LIST);
        histogram.setBinUnit((double) modulo);
        histogram.setBinUnitLabel("DATE");
        histogram.setDataUnitLabel("Avg for Interval");
        // 48 hours X 12 + 1
        histogram.setBinCount(577);
        histogram.setHighestBin(highestBin);
        histogram.setIdentifier(identifier);
        histogram.configure();
        return histogram;
    }

    /**
     * <p>If the model does not exist, a new one is created.</p>
     * <p>
     * <p>The model is then updated with the provided value.</p>
     *
     * @param series    the type of histogram (max, min, avg, stddev)
     * @param newValue  for the update
     * @param model     the current model, if it exists
     * @param timestamp of update
     * @return the current or new model
     */
    public static TimeseriesHistogram updateFileLifetimeSeries(String series,
                                                               Double newValue,
                                                               TimeseriesHistogram model,
                                                               long timestamp) {
        if (model == null) {
            model = newLifetimeTimeSeriesHistogram(series, series);
        }

        model.replace(newValue, timestamp);

        return model;
    }

    /**
     * <p>From the lifetime (last accessed) time interval values, builds
     * a new binned histogram and updates the time series tracking maximum,
     * minimum, average and standard deviation for file lifetime. These
     * are added to /updated on the persistent pool data object.</p>
     *
     * @param lifetimeHistogram of the (current) files in the pool's repository
     * @param info              the storage object to update
     * @param timestamp         of update
     */
    public static void updateFstatHistograms(
                    CountingHistogram lifetimeHistogram,
                    PoolInfoWrapper info,
                    long timestamp) {
        updateFstatTimeSeries(lifetimeHistogram.getMetadata(), info, timestamp);
    }

    /**
     * <p>Extracts the statistics from the model and updates the
     * time series histograms.</p>
     *
     * @param metadata  containing the relevant statistics (usually the
     *                  binned counts)
     * @param info      persistent/cached data for the pool
     * @param timestamp of update
     */
    public static void updateFstatTimeSeries(HistogramMetadata metadata,
                                             PoolInfoWrapper info,
                                             long timestamp) {
        long count = metadata.getCount();
        Double min = metadata.getMinValue().orElse(null);
        Double max = metadata.getMaxValue().orElse(null);
        double stddev = metadata.standardDeviation();

        if (count != 0L) {
            double avg = metadata.getSum() / count;
            info.setFileLiftimeAvg(updateFileLifetimeSeries("AVG", avg,
                                                            info.getFileLiftimeAvg(),
                                                            timestamp));
        }

        info.setFileLiftimeMax(updateFileLifetimeSeries("MAX", max,
                                                        info.getFileLiftimeMax(),
                                                        timestamp));
        info.setFileLiftimeMin(updateFileLifetimeSeries("MIN", min,
                                                        info.getFileLiftimeMin(),
                                                        timestamp));
        info.setFileLiftimeStddev(updateFileLifetimeSeries("STD DEV", stddev,
                                                           info.getFileLiftimeStddev(),
                                                           timestamp));
    }

    /**
     * <p>Aggregate version.</p>
     *
     * @param poolInfo  containing the data for pools in the group.
     * @param groupInfo the storage object to update
     * @param timestamp of update
     */
    public static void updateQstatTimeSeries(List<PoolInfoWrapper> poolInfo,
                                             PoolInfoWrapper groupInfo,
                                             long timestamp) {
        updateQstatTimeSeries(new QueueData(poolInfo), groupInfo, timestamp);
    }

    /**
     * <p>From the latest pool queue values, updates or creates the time series
     * tracking each type. These are added to / updated on the persistent
     * pool data object.</p>
     *
     * @param poolCostData containing the data for pool queues.
     * @param info         the storage object to update
     * @param timestamp    of update
     */
    public static void updateQstatTimeSeries(PoolCostData poolCostData,
                                             PoolInfoWrapper info,
                                             long timestamp) {
        updateQstatTimeSeries(new QueueData(poolCostData), info, timestamp);
    }

    /**
     * <p>If the model does not exist, a new one is created.</p>
     *
     * <p>The model is then updated with the provided value.  The current
     * value replaces rather than averages because we want to see
     * when the queues drop to an effective zero.</p>
     *
     * @param series       active or queued movers, p2ps, p2pclients, stores or restores
     * @param currentCount the number of requests running or waiting
     * @param model        the current model, if it exists
     * @param timestamp    of update
     * @return the current or new model
     */
    public static TimeseriesHistogram updateQueueSeries(String series,
                                                        Double currentCount,
                                                        TimeseriesHistogram model,
                                                        long timestamp) {
        if (model == null) {
            model = newQueueTimeSeriesHistogram(series);
        }

        if (currentCount != null) {
            model.replace(currentCount, timestamp);
        }

        return model;
    }

    private static void updateQstatTimeSeries(QueueData data,
                                              PoolInfoWrapper info,
                                              long timestamp) {
        info.setActiveMovers(updateQueueSeries("Active Movers",
                                               data.activeMovers,
                                               info.getActiveMovers(),
                                               timestamp));
        info.setQueuedMovers(updateQueueSeries("Queued Movers",
                                               data.queuedMovers,
                                               info.getQueuedMovers(),
                                               timestamp));
        info.setActiveP2P(updateQueueSeries("Active P2P",
                                            data.activeP2P,
                                            info.getActiveP2P(),
                                            timestamp));
        info.setQueuedP2P(updateQueueSeries("Queued P2P",
                                            data.queuedP2P,
                                            info.getQueuedP2P(),
                                            timestamp));
        info.setActiveP2PClient(updateQueueSeries("Active P2P Client",
                                                  data.activeP2PClient,
                                                  info.getActiveP2PClient(),
                                                  timestamp));
        info.setQueuedP2PClient(updateQueueSeries("Queued P2P Client",
                                                  data.queuedP2PClient,
                                                  info.getQueuedP2PClient(),
                                                  timestamp));
        info.setActiveFlush(updateQueueSeries("Active Stores",
                                              data.activeFlush,
                                              info.getActiveFlush(),
                                              timestamp));
        info.setQueuedFlush(updateQueueSeries("Queued Stores",
                                              data.queuedFlush,
                                              info.getQueuedFlush(),
                                              timestamp));
        info.setActiveStage(updateQueueSeries("Active Restores",
                                              data.activeStage,
                                              info.getActiveStage(),
                                              timestamp));
        info.setQueuedStage(updateQueueSeries("Queued Restores",
                                              data.queuedStage,
                                              info.getQueuedStage(),
                                              timestamp));
    }
}
