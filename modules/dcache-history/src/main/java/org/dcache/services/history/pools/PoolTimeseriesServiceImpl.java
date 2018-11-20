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
package org.dcache.services.history.pools;

import org.springframework.beans.factory.annotation.Required;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.Reply;

import org.dcache.cells.MessageReply;
import org.dcache.pool.json.PoolInfoWrapper;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.services.collector.CellDataCollectingService;
import org.dcache.util.collector.ListenableFutureWrapper;
import org.dcache.util.collector.pools.PoolInfoCollectorUtils;
import org.dcache.util.collector.pools.PoolLiveDataCollector;
import org.dcache.util.histograms.TimeseriesHistogram;
import org.dcache.vehicles.histograms.AggregateFileLifetimeRequestMessage;
import org.dcache.vehicles.histograms.PoolTimeseriesRequestMessage;
import org.dcache.vehicles.histograms.PoolTimeseriesRequestMessage.TimeseriesType;
import org.dcache.vehicles.pool.PoolLiveDataForHistoriesMessage;

/**
 * <p>This is an implementation of the {@link PoolTimeseriesService} interface
 * which uses the {@link CellDataCollectingService} abstraction in order
 * to maintain and serve local file copies of the JSON objects containing
 * historical/timeseries pool data.</p>
 */
public final class PoolTimeseriesServiceImpl extends
                CellDataCollectingService<Map<String, ListenableFutureWrapper<PoolLiveDataForHistoriesMessage>>,
                                PoolLiveDataCollector>
                implements CellMessageReceiver, PoolListingService {
    private final Map<String, PoolInfoWrapper> cache = new HashMap<>();

    private PoolHistoriesRequestProcessor processor;

    private PoolMonitor monitor;

    protected Executor executor;

    @Override
    public void configure() {
        synchronized (cache) {
            cache.putAll(processor.readFromDisk());
        }
    }

    public Map<TimeseriesType, TimeseriesHistogram> getTimeseries(String key,
                                                                  Set<TimeseriesType> types) {
        Map<TimeseriesType, TimeseriesHistogram> histograms = new HashMap<>();

        PoolInfoWrapper info = getWrapper(key);

        if (info != null) {
            histograms.put(TimeseriesType.ACTIVE_FLUSH, info.getActiveFlush());
            histograms.put(TimeseriesType.ACTIVE_MOVERS,
                           info.getActiveMovers());
            histograms.put(TimeseriesType.ACTIVE_P2P, info.getActiveP2P());
            histograms.put(TimeseriesType.ACTIVE_P2P_CLIENT,
                           info.getActiveP2PClient());
            histograms.put(TimeseriesType.ACTIVE_STAGE, info.getActiveStage());
            histograms.put(TimeseriesType.QUEUED_FLUSH, info.getQueuedFlush());
            histograms.put(TimeseriesType.QUEUED_MOVERS,
                           info.getQueuedMovers());
            histograms.put(TimeseriesType.QUEUED_P2P, info.getQueuedP2P());
            histograms.put(TimeseriesType.QUEUED_P2P_CLIENT,
                           info.getQueuedP2PClient());
            histograms.put(TimeseriesType.QUEUED_STAGE, info.getQueuedStage());
            histograms.put(TimeseriesType.FILE_LIFETIME_MAX,
                           info.getFileLiftimeMax());
            histograms.put(TimeseriesType.FILE_LIFETIME_AVG,
                           info.getFileLiftimeAvg());
            histograms.put(TimeseriesType.FILE_LIFETIME_MIN,
                           info.getFileLiftimeMin());
            histograms.put(TimeseriesType.FILE_LIFETIME_STDDEV,
                           info.getFileLiftimeStddev());
        }

        return histograms;
    }

    public PoolInfoWrapper getWrapper(String key) {
        synchronized (cache) {
            return cache.get(key);
        }
    }

    public PoolSelectionUnit getSelectionUnit() {
        return monitor.getPoolSelectionUnit();
    }

    @Override
    public String[] listGroups() {
        return PoolInfoCollectorUtils.listGroups(monitor.getPoolSelectionUnit());
    }

    @Override
    public String[] listPools(String group) {
        return PoolInfoCollectorUtils.listPools(group, monitor.getPoolSelectionUnit());
    }

    @Override
    public String[] listPools() {
        return PoolInfoCollectorUtils.listPools(monitor.getPoolSelectionUnit());
    }

    public Set<String> validKeys() {
        Set<String> keys = new HashSet<>();
        PoolSelectionUnit psu = monitor.getPoolSelectionUnit();
        keys.addAll(psu.getPools().keySet());
        keys.addAll(psu.getPoolGroups().keySet());
        keys.add(ALL);
        return keys;
    }

    public Reply messageArrived(PoolTimeseriesRequestMessage message) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                message.setHistogramMap(getTimeseries(message.getPool(),
                                                      message.getKeys()));
                reply.reply(message);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(AggregateFileLifetimeRequestMessage message) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                message.setAggregateLifetime(getWrapper(message.getPoolGroup())
                                                               .getInfo()
                                                               .getSweeperData()
                                                               .getLastAccessHistogram());
                reply.reply(message);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
        return reply;
    }

    @Required
    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    @Required
    public void setPoolMonitor(PoolMonitor monitor) {
        this.monitor = monitor;
    }

    @Required
    public void setProcessor(PoolHistoriesRequestProcessor processor) {
        this.processor = processor;
    }

    /**
     * <p>Callback invoked by processor when it has completed
     * the update.</p>
     *
     * @param next updated map data.
     */
    public void updateJsonData(Map<String, PoolInfoWrapper> next) {
        synchronized (cache) {
            /*
             *  If a pool goes offline, then the message requesting data
             *  will result in a null entry in the map.   We do not want to
             *  overwrite data in the current cache with this null because
             *  this will cause the history to be lost when the map is written
             *  to disk.
             *
             *  At the same time, we do not want to have to reread the saved
             *  data from disk at each collection.
             *
             *  However, if we simply overwrite without clearing the cache,
             *  we could find that pools which have been eliminated still
             *  continue to be reported.
             *
             *  We thus need to prune the cache against the listed
             *  pools and groups.
             */
            cache.putAll(next);
            Set<String> valid = validKeys();
            for (Iterator i = cache.keySet().iterator(); i.hasNext();) {
                if (!valid.contains(i.next())) {
                    i.remove();
                }
            }
        }
    }

    @Override
    protected void update(
                    Map<String, ListenableFutureWrapper<PoolLiveDataForHistoriesMessage>> data) {
        processor.process(data);
    }
}
