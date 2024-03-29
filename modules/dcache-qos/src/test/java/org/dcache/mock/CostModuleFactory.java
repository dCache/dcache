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
package org.dcache.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.PoolCostInfo;
import dmg.cells.nucleus.CellAddressCore;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.dcache.pool.classic.IoQueueManager;
import org.dcache.poolmanager.PoolInfo;

final class CostModuleFactory {

    static class TestCostModule implements CostModule {
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
            pools.forEach((p) -> map.put(p, poolInfoMap.get(p)));
            return map;
        }

        @Override
        public double getPoolsPercentilePerformanceCost(double fraction) {
            return 0;
        }

        void addPool(TestPoolInfo pool) {
            String name = pool.getPool();
            int[] metric = pool.getCost();
            long[] space = pool.getSpace();
            PoolCostInfo cost = buildPoolCost(name, metric[0], metric[1], metric[2], metric[3],
                  metric[4], metric[5], metric[6], metric[7], metric[8], space[0], space[1], space[2],
                  space[3]);
            pools.add(name);
            poolCostInfoMap.put(name, cost);
            PoolInfo info = new PoolInfo(new CellAddressCore(name), cost,
                  ImmutableMap.copyOf(pool.getTags()));
            poolInfoMap.put(name, info);
        }
    }

    public static CostModule create(String resourcePath) {
        InputStream is = CostModuleFactory.class.getClassLoader().getResourceAsStream(resourcePath);
        Reader reader = new BufferedReader(new InputStreamReader(is));
        List<TestPoolInfo> users
              = new Gson().fromJson(reader, new TypeToken<List<TestPoolInfo>>() {}.getType());
        TestCostModule costModule = new TestCostModule();
        users.forEach(costModule::addPool);
        return costModule;
    }

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
}
