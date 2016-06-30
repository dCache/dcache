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
package org.dcache.resilience;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.PoolCostInfo;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;
import org.dcache.pool.classic.IoQueueManager;
import org.dcache.poolmanager.PoolInfo;

final class TestCostModule implements CostModule {
    private static PoolCostInfo buildPoolCost(String name,
                                              int moverActive,
                                              int moverMaxActive,
                                              int moverQueued,
                                              int restoreActive,
                                              int restoreMaxActive,
                                              int restoreQueued,
                                              int storeActive,
                                              int storeMaxActive,
                                              int storeQueued,
                                              long total,
                                              long free,
                                              long precious,
                                              long removable) {
        PoolCostInfo poolCost
                        = new PoolCostInfo(name, IoQueueManager.DEFAULT_QUEUE);

        poolCost.setQueueSizes(restoreActive,
                               restoreMaxActive,
                               restoreQueued,
                               storeActive,
                               storeMaxActive,
                               storeQueued);

        poolCost.addExtendedMoverQueueSizes(IoQueueManager.DEFAULT_QUEUE,
                                            moverActive,
                                            moverMaxActive,
                                            moverQueued,
                                            0, 0);

        poolCost.setSpaceUsage(total, free, precious, removable);

        return poolCost;
    }

    private static ImmutableMap<String, String> getTagMap(int i) {
        Map<String, String> map = new HashMap<>();
        String[] pooltags = TestData.POOL_TAGS[i];
        for (String tag: pooltags) {
            String[] keyValue = tag.split("[:]");
            map.put(keyValue[0], keyValue[1]);
        }
        return ImmutableMap.copyOf(map);
    }

    final Set<String> pools = new HashSet<>();
    final Map<String, PoolInfo> poolInfoMap = new HashMap<>();
    final Map<String, PoolCostInfo> poolCostInfoMap = new HashMap<>();

    @Override
    public PoolCostInfo getPoolCostInfo(String poolName) {
        return poolCostInfoMap.get(poolName);
    }

    @Nullable
    @Override
    public PoolInfo getPoolInfo(String pool) {
        return poolInfoMap.get(pool);
    }

    @Override
    public Collection<PoolCostInfo> getPoolCostInfos() {
        return ImmutableList.copyOf(poolCostInfoMap.values());
    }

    @Override
    public Map<String, PoolInfo> getPoolInfoAsMap(Iterable<String> pools) {
        Map<String, PoolInfo> map = new HashMap<>();
        pools.forEach((p)->map.put(p, poolInfoMap.get(p)));
        return map;
    }

    @Override
    public double getPoolsPercentilePerformanceCost(double fraction) {
        return 0;
    }

    @Override
    public void messageArrived(CellMessage cellMessage) {
        //NOP
    }

    String dumpInfo() {
        StringBuilder builder = new StringBuilder();
        String[] keys = poolInfoMap.keySet().toArray(new String[0]);
        Arrays.sort(keys);
        for (String pool: keys) {
            builder.append(pool).append("\n");
            PoolInfo info = poolInfoMap.get(pool);
            builder.append("\tAddress:\t:").append(info.getAddress()).append("\n");
            builder.append("\tCost Info:\t:").append(info.getCostInfo()).append("\n");
            builder.append("\tPool Tags:\t:").append(info.getTags()).append("\n");
        }
        return builder.toString();
    }

    TestCostModule load() {
        for (int k = 0; k < TestData.POOL_TYPE.length; ++k) {
            for (int i = 0; i < TestData.POOL_COUNT[k]; ++i) {
                String name = TestData.POOL_TYPE[k] + i;
                addPool(name, i);
            }
        }

        return this;
    }

    void addPool(String name, int index) {
        int [] metric = TestData.COST_METRICS[index];
        long[] space = TestData.POOL_SPACE[index];
        PoolCostInfo cost = buildPoolCost(name, metric[0],
                        metric[1],
                        metric[2],
                        metric[3],
                        metric[4],
                        metric[5],
                        metric[6],
                        metric[7],
                        metric[8],
                        space[0],
                        space[1],
                        space[2],
                        space[3]);
        pools.add(name);
        poolCostInfoMap.put(name, cost);
        PoolInfo info = new PoolInfo(new CellAddressCore(name),
                        cost,
                        getTagMap(index));
        poolInfoMap.put(name, info);
    }
}
