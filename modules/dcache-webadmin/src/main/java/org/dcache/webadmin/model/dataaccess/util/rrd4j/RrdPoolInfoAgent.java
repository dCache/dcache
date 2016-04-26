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
package org.dcache.webadmin.model.dataaccess.util.rrd4j;

import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;
import org.rrd4j.core.Sample;
import org.rrd4j.core.Util;
import org.rrd4j.graph.RrdGraph;
import org.rrd4j.graph.RrdGraphDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.CacheException;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.webadmin.model.businessobjects.rrd4j.PoolQueuePlotData;
import org.dcache.webadmin.model.businessobjects.rrd4j.PoolQueuePlotData.PoolQueueHistogram;

import static org.rrd4j.ConsolFun.LAST;
import static org.rrd4j.DsType.GAUGE;

/**
 * Responsible for processing periodically collected pool queue statistics and
 * storing them in a round-robin database, Plots are immediately regenerated
 * after each collection using <code>org.rrd4j</code>.
 *
 * @author arossi
 */
public class RrdPoolInfoAgent implements Runnable {
    private static final FilenameFilter FILES = (dir, name) ->
                    name.contains(RrdSettings.FILE_SUFFIX)
                                    || name.endsWith(RrdSettings.RRD_SUFFIX);

    private static final FileFilter GROUP_DIRS = (file) ->
                    file.isDirectory() && file.getName()
                                              .contains(RrdSettings.GROUP_DIR);

    private static final Logger logger
        = LoggerFactory.getLogger(RrdPoolInfoAgent.class);

    private static void updatePoolGroupData(String group,
                    Map<String, PoolQueuePlotData> dataMap,
                    PoolCostInfo costInfo) {
        PoolQueuePlotData data = dataMap.get(group);
        if (data == null) {
            data = new PoolQueuePlotData(group);
            dataMap.put(group, data);
        }
        data.addValues(costInfo);
    }

    /**
     * injected
     */
    private RrdSettings settings;

    /**
     * state
     */
    private PoolSelectionUnit poolSelectionUnit;
    private CostModule        costModule;
    private Set<String>       current;
    private long              lastRefresh;
    private Thread            refresher;

    public void initialize() {
        current = new HashSet<>();
        settings.initialize();
        logger.info(settings.toString());
    }

    public void notify(PoolMonitor monitor) {
        long now = System.currentTimeMillis();
        synchronized (this) {
            if (now - lastRefresh >= settings.stepInMillis) {
                if (refresher == null || !refresher.isAlive()) {
                    poolSelectionUnit = monitor.getPoolSelectionUnit();
                    costModule = monitor.getCostModule();
                    lastRefresh = now;
                    refresher = new Thread(this);
                    refresher.start();
                }
            }
        }
    }

    @Override
    public void run() {
        try {
            processPools();
        } catch (InterruptedException t) {
            logger.error("pool queue plot update interrupted ...");
            return;
        } catch (CacheException t) {
            logger.error("problem updating pool queue plots: {}",
                            t.getMessage());
            return;
        }

        File base = new File(settings.baseDirectory);

        createPlot(base, RrdSettings.ALL_POOLS);

        poolSelectionUnit.getPoolGroups().values()
                         .stream()
                         .map(SelectionPoolGroup::getName)
                         .forEach((g) ->
                         {
                             createPlot(base, g);
                             File group = new File(base, RrdSettings.GROUP_DIR + g);
                             group.mkdirs();
                             poolSelectionUnit.getPoolsByPoolGroup(g)
                                              .stream()
                                              .map(SelectionPool::getName)
                                              .forEach((p) ->createPlot(group, p));
                         });

        removeStale();
    }

    public void setSettings(RrdSettings settings) {
        this.settings = settings;
    }

    /**
     * Generate the graph from the database.
     */
    private void createPlot(File dir, String name) {
        try {
            new RrdGraph(getGraphDef(dir, name));
            logger.debug("created plot for {}", name);
        } catch (IOException t) {
            logger.error("problem during plot creation for {}: {}",
                            name, t.getMessage());
        }
    }

    /**
     * aligns the update time to the nearest step, in order to preserve
     * non-fractional (non-interpolated) values. This is necessary because Rrd4j
     * computes the current slot value by interpolating against an interval
     * endpoint which is aligned modulo step-size.
     */
    private long getAlignedCurrentTimeInSecs() {
        return Util.normalize(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                              settings.stepInSeconds);
    }

    /**
     * Returns database constructed from the binary .rrd file if it exists; if
     * not, constructs a new one from the settings.
     */
    private RrdDb getDatabase(String name, boolean readOnly) throws IOException {
        String rrdPath = new File(settings.baseDirectory,
                                    name + RrdSettings.RRD_SUFFIX)
                                    .getAbsolutePath();
        current.add(rrdPath);
        File rrd = new File(rrdPath);
        if (rrd.exists()) {
            return new RrdDb(rrdPath, readOnly);
        }

        long endInSeconds = getAlignedCurrentTimeInSecs();
        long startInSeconds = endInSeconds - settings.spanInSeconds
                        + settings.stepInSeconds;

        RrdDef rrdDef = new RrdDef(rrdPath, startInSeconds,
                        settings.stepInSeconds, settings.version);

        long heartbeat = (long) (settings.stepInSeconds * settings.heartbeatFactor);
        for (PoolQueueHistogram h : PoolQueueHistogram.values()) {
            rrdDef.addDatasource(h.getSourceName(),
                                 GAUGE,
                                 heartbeat,
                                 0,
                                 Double.MAX_VALUE);
        }

        rrdDef.addArchive(LAST, 0.5, 1, settings.numSteps);
        new RrdDb(rrdDef).close();
        return new RrdDb(rrdPath, readOnly);
    }

    /**
     * Currently set to use only one archive (actual values) for each histogram.
     * Creates a stacked area graph.
     */
    private RrdGraphDef getGraphDef(File dir, String name) throws IOException {
        String imgPath = RrdSettings.getImagePath(settings.imgType, dir, name)
                                    .getAbsolutePath();
        current.add(imgPath);
        RrdDb rrdDb = getDatabase(name, true);
        RrdGraphDef gDef = new RrdGraphDef();
        String rrdPath = rrdDb.getPath();

        gDef.setWidth(settings.imgWidth);
        gDef.setHeight(settings.imgHeight);
        gDef.setFilename(imgPath);
        long graphStart = getAlignedCurrentTimeInSecs()
                        - settings.spanInSeconds
                        + settings.rightMarginInSeconds;
        gDef.setStartTime(graphStart);
        gDef.setEndTime(graphStart + settings.spanInSeconds);
        gDef.setStep(settings.stepInSeconds);
        gDef.setTitle(name);
        gDef.setVerticalLabel(settings.yLabel);
        gDef.setImageQuality(1F);
        gDef.setMinValue(0);
        gDef.setTimeAxis(settings.minorUnit, settings.minorUnitCount,
                        settings.majorUnit, settings.majorUnitCount,
                        settings.labelUnit, settings.labelUnitCount,
                        settings.labelSpan, settings.simpleDateFormat);

        PoolQueueHistogram[] values = PoolQueueHistogram.values();
        PoolQueueHistogram h = values[0];
        String srcName = h.getSourceName();
        gDef.datasource(srcName, rrdPath, srcName, LAST);
        gDef.area(srcName, h.getColor(), h.getLabel());

        for (int i = 1; i < values.length; i++) {
            h = values[i];
            srcName = h.getSourceName();
            gDef.datasource(srcName, rrdPath, srcName, LAST);
            gDef.stack(srcName, h.getColor(), h.getLabel());
        }

        gDef.setImageInfo("<img src='%s' width='%d' height = '%d'>");
        gDef.setPoolUsed(false);
        gDef.setImageFormat(settings.imgType);

        logger.debug("got graph definition for {}", name);
        rrdDb.close();

        return gDef;
    }

    /**
     * Creates storage objects from the pool cost info and then calls
     * {@link #storeToRRD(PoolQueuePlotData, long)} to record the data in the
     * round-robin database.
     */
    private void processPools() throws InterruptedException, CacheException {
        PoolQueuePlotData all = new PoolQueuePlotData(RrdSettings.ALL_POOLS);

        long now = getAlignedCurrentTimeInSecs();

        Collection<SelectionPool> pools
                        = poolSelectionUnit.getAllDefinedPools(false);
        Map<String, PoolQueuePlotData> groupData = new HashMap<>();

        for (SelectionPool selectionPool : pools) {
            String poolName = selectionPool.getName();

            PoolCostInfo costInfo = costModule.getPoolCostInfo(poolName);
            if (costInfo == null) {
                continue;
            }

            storeToRRD(new PoolQueuePlotData(costInfo), now);
            logger.debug("successfully wrote pool queue data for {}",
                            selectionPool.getName());

            poolSelectionUnit.getPoolGroupsOfPool(poolName).stream()
                             .forEach((g) -> updatePoolGroupData(g.getName(),
                                                                 groupData,
                                                                 costInfo));

            all.addValues(costInfo);
        }

        if (!groupData.isEmpty()) {
            groupData.values().stream().forEach((d) -> storeToRRD(d, now));
            logger.debug("successfully wrote pool queue data for groups");

            storeToRRD(all, now);
            logger.debug("successfully wrote pool queue data for {}",
                            RrdSettings.ALL_POOLS);
        }
    }

    /**
     * If the plots and database files do not appear in the current
     * list based on the most recent pool selection unit values, remove
     * them.
     */
    private void removeStale() {
        File top = new File(settings.baseDirectory);
        removeStale(top);

        /*
         *  Depth is only 1 (base has all the database files and groups,
         *  the group directories contain the plots).
         */
        File[] groupDirs = top.listFiles(GROUP_DIRS);
        for (File dir: groupDirs) {
            removeStale(dir);
            File[] children = dir.listFiles();
            /*
             * Children cannot be null here, since the IO issue would already
             * have been detected.
             */
            if (children.length == 0) {
                dir.delete();
            }
        }

        current.clear();
    }

    /*
     *  Removes the stale db files, and the stale group queue plots (when
     *  applied to top level) or the stale pool queue plots (when applied to
     *  group directories).
     */
    private void removeStale(File dir) {
        File[] files = dir.listFiles(FILES);
        for (File file: files) {
            if (!current.contains(file.getAbsolutePath())) {
                file.delete();
            }
        }
    }

    /**
     * Stores timestamp and six values for each pool (stored/active movers,
     * stores and restores).
     */
    private void storeToRRD(PoolQueuePlotData data, long now) {
        String name = data.getPoolName();
        RrdDb rrdDb;
        try {
            rrdDb = getDatabase(name, false);
        } catch (IOException t) {
            logger.error("could not open RrdDb file for {}: {}",
                            name, t.getMessage());
            return;
        }

        try {
            if (now - rrdDb.getLastUpdateTime() >= 1) {
                Sample sample = rrdDb.createSample();
                sample.setTime(now);
                Map<String, Double> values = data.data();

                for (PoolQueueHistogram h : PoolQueueHistogram.values()) {
                    String src = h.getSourceName();
                    sample.setValue(src, values.get(src));
                }

                logger.debug("{}\t{}", new Date(TimeUnit.SECONDS.toMillis(now)),
                                sample.dump());

                sample.update();
                logger.trace(rrdDb.dump());
            }
        } catch (IOException t) {
            logger.error("problem writing data to RrdDb: {}", t.getMessage());
        } finally {
            try {
                rrdDb.close();
            } catch (IOException t) {
                logger.error("problem closing RrdDb: {}", t.getMessage());
            }
        }
    }
}
