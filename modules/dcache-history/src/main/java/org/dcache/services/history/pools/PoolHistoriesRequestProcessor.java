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

import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import org.springframework.beans.factory.annotation.Required;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import diskCacheV111.pools.json.PoolCostData;
import diskCacheV111.util.CacheException;

import org.dcache.pool.classic.json.SweeperData;
import org.dcache.pool.json.PoolData;
import org.dcache.pool.json.PoolDataDetails;
import org.dcache.pool.json.PoolInfoWrapper;
import org.dcache.util.collector.RequestFutureProcessor;
import org.dcache.util.collector.pools.PoolHistoriesAggregator;
import org.dcache.util.collector.pools.PoolInfoCollectorUtils;
import org.dcache.util.histograms.CountingHistogram;
import org.dcache.vehicles.pool.PoolLiveDataForHistoriesMessage;

/**
 * <p>Handles the transformation of message content from pools into a cached
 * {@link PoolInfoWrapper}.  The data handled by this transformation are
 * the timeseries histograms for request queues and for file lifetime.</p>
 *
 * <p>Post-processing stores all data to local files that are read back
 * in on start-up.</p>
 */
public final class PoolHistoriesRequestProcessor extends
                RequestFutureProcessor<PoolInfoWrapper, PoolLiveDataForHistoriesMessage> {
    private static final FilenameFilter filter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".json");
        }
    };

    private PoolTimeseriesServiceImpl service;
    private PoolHistoriesAggregator   handler;
    private File                      storageDir;

    /**
     * <p>It is assumed the storage directory points to a shallow directory
     * containing files which have the ".json" extension and whose
     * names correspond to the key values of the map.</p>
     *
     * @return map of JSON values and filename (minus extension) keys.
     */
    public Map<String, PoolInfoWrapper> readFromDisk() {
        /*
         *  This should only happen if the default dCache property value
         *  is not used and the non-standard directory does not yet exist.
         */
        if (!storageDir.exists()) {
            storageDir.mkdirs();
            return Collections.EMPTY_MAP;
        }

        Map<String, PoolInfoWrapper> values = new HashMap<>();

        File[] files = storageDir.listFiles(filter);

        PoolInfoWrapper info;
        String key;

        GsonBuilder builder = new GsonBuilder();

        for (File file : files) {
            info = null;
            key = null;

            try (FileReader reader = new FileReader(file)) {
                key = file.getName();
                if (key.contains(".")) {
                    key = key.substring(0, key.lastIndexOf("."));
                }

                info = builder.create().fromJson(reader,
                                                 PoolInfoWrapper.class);
            } catch (JsonSyntaxException | JsonIOException e) {
                LOGGER.warn("Json parsing/syntax problem for {}: {}; file "
                                            + "is corrupt or incomplete; removing ...",
                            file, e.getMessage());
                file.delete();
            } catch (IOException e) {
                LOGGER.warn("There was a problem reading json file {}: {}.",
                            file, e.getMessage());
            }

            if (key != null) {
                values.put(key, info == null ? new PoolInfoWrapper() : info);
            }
        }

        return values;
    }

    @Required
    public void setHandler(PoolHistoriesAggregator handler) {
        this.handler = handler;
    }

    @Required
    public void setService(PoolTimeseriesServiceImpl service) {
        this.service = service;
    }

    @Required
    public void setStorageDir(File storageDir) {
        this.storageDir = storageDir;
    }

    @Override
    protected void postProcess() {
        try {
            handler.aggregateDataForPoolGroups(next,
                                               service.getSelectionUnit());
        } catch (CacheException e) {
            LOGGER.error("Could not add aggregate data for pool groups: {}.",
                         e.getMessage());
        }

        writeMapToDisk();
        service.updateJsonData(next);
    }

    @Override
    protected PoolInfoWrapper process(String key,
                                      PoolLiveDataForHistoriesMessage data,
                                      long sent) {
        Serializable errorObject = data.getErrorObject();

        if (errorObject != null) {
            LOGGER.warn("Problem with retrieval of live pool data for {}: {}.",
                        key, errorObject.toString());
            return null;
        }

        PoolInfoWrapper info = service.getWrapper(key);

        if (info == null) {
            info = new PoolInfoWrapper();
            info.setKey(key);
        }

        long timestamp = System.currentTimeMillis();

        PoolCostData poolCostData = data.getPoolCostData();

        if (poolCostData != null) {
            PoolInfoCollectorUtils.updateQstatTimeSeries(poolCostData,
                                                         info,
                                                         timestamp);
        }

        SweeperData sweeperData = data.getSweeperData();
        CountingHistogram histogram = sweeperData == null ? null :
                        sweeperData.getLastAccessHistogram();

        if (histogram != null) {
            PoolInfoCollectorUtils.updateFstatHistograms(histogram,
                                                         info,
                                                         timestamp);
        }

        PoolData poolData = new PoolData();
        PoolDataDetails details = new PoolDataDetails();
        poolData.setDetailsData(details);
        details.setCostData(poolCostData);
        poolData.setSweeperData(sweeperData);
        info.setInfo(poolData);

        return info;
    }

    private void writeMapToDisk() {
        GsonBuilder builder = new GsonBuilder().setPrettyPrinting();
        for (Entry<String, PoolInfoWrapper> entry : next.entrySet()) {
            File file = new File(storageDir, entry.getKey() + ".json");
            try (FileWriter writer = new FileWriter(file, false)) {
                builder.create().toJson(entry.getValue(), writer);
            } catch (IOException e) {
                LOGGER.warn("There was a problem serializing json to file {}: "
                                            + "{}, {}",
                            file, e.getMessage(), e.getCause());
            }
        }
    }
}
