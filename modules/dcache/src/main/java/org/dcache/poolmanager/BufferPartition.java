/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.poolmanager;

import com.google.common.collect.ImmutableMap;
import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.CacheException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.dcache.pool.assumption.AvailableSpaceAssumption;
import org.dcache.vehicles.FileAttributes;

/**
 * This partition type is for pools that are used as temporary transfer buffers rather than for long
 * term storage.
 * <p>
 * Pool selection does not take free space into account (with the exception of not selecting full
 * pools for writes). Pools are selected by load only, using a randomized weighted selection with
 * 1/2^(cc * mc * n) as the weigth, where n is the number of movers on the pool, mc is the mover
 * cost factor of the pool and cc is the cpucostfactor as configured in the partition. Mover queue
 * limits do not influence pool selection.
 * <p>
 * Buffer pools do not support hot spot replication, cost based link fallbacks, replication limits,
 * same host replication protection, nor idle cost cuts. Buffer pools do support pool to pool
 * transfers as well as stage from tape, although the current implementation may not be well suited
 * for tape pools.
 */
public class BufferPartition extends Partition {

    private static final long serialVersionUID = 437942839152498525L;

    private static final SecureRandom RANDOM = new SecureRandom();

    static final String TYPE = "buffer";

    private static final Map<String, String> DEFAULTS =
          ImmutableMap.of("cpucostfactor", "1.0");
    private final double costFactor;

    public BufferPartition(Map<String, String> inherited) {
        this(inherited, Partition.NO_PROPERTIES);
    }

    public BufferPartition(Map<String, String> inherited, Map<String, String> defined) {
        super(DEFAULTS, inherited, defined);
        costFactor = getDouble("cpucostfactor");
    }

    @Override
    protected Partition create(Map<String, String> inherited, Map<String, String> defined) {
        return new BufferPartition(inherited, defined);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private double random() {
        return RANDOM.nextDouble();
    }

    private double getWeight(PoolCostInfo cost) {
        int movers = 0;
        for (PoolCostInfo.PoolQueueInfo queue : cost.getExtendedMoverHash().values()) {
            movers += queue.getActive() + queue.getQueued();
        }
        return 1.0 / (Math.pow(2.0, (cost.getMoverCostFactor() * costFactor * movers)));
    }

    private SelectedPool selectPool(List<PoolInfo> pools) {
        double[] available = new double[pools.size()];
        double sum = 0.0;

        for (int i = 0; i < available.length; i++) {
            sum += getWeight(pools.get(i).getCostInfo());
            available[i] = sum;
        }

        double threshold = random() * sum;

        for (int i = 0; i < available.length; i++) {
            if (threshold < available[i]) {
                return new SelectedPool(pools.get(i));
            }
        }

        return null;
    }

    private SelectedPool selectPool(List<PoolInfo> pools, final long preallocated) {
        double[] available = new double[pools.size()];
        double sum = 0.0;

        for (int i = 0; i < available.length; i++) {
            PoolInfo pool = pools.get(i);
            PoolCostInfo.PoolSpaceInfo space = pool.getCostInfo().getSpaceInfo();
            if (space.getFreeSpace() + space.getRemovableSpace() - space.getGap() > preallocated) {
                sum += getWeight(pool.getCostInfo());
                available[i] = sum;
            }
        }

        double threshold = random() * sum;

        for (int i = 0; i < available.length; i++) {
            if (threshold < available[i]) {
                return new SelectedPool(pools.get(i), new AvailableSpaceAssumption(preallocated));
            }
        }

        return null;
    }

    @Override
    public SelectedPool selectWritePool(CostModule cm, List<PoolInfo> pools,
          FileAttributes attributes,
          long preallocated) throws CacheException {
        return selectPool(pools, preallocated);
    }

    @Override
    public SelectedPool selectReadPool(CostModule cm, List<PoolInfo> pools,
          FileAttributes attributes) throws CacheException {
        return selectPool(pools);
    }

    @Override
    public P2pPair selectPool2Pool(CostModule cm, List<PoolInfo> src, List<PoolInfo> dst,
          FileAttributes attributes, boolean force) throws CacheException {
        return new P2pPair(selectPool(src), selectPool(dst, attributes.getSize()));
    }

    @Override
    public SelectedPool selectStagePool(CostModule cm, List<PoolInfo> pools,
          Optional<PoolInfo> previous,
          FileAttributes attributes) throws CacheException {
        return selectPool(pools, attributes.getSize());
    }
}
