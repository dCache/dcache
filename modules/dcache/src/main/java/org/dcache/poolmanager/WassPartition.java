package org.dcache.poolmanager;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.DestinationCostException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.SourceCostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.dcache.pool.assumption.Assumption;
import org.dcache.pool.assumption.Assumptions;
import org.dcache.pool.assumption.AvailableSpaceAssumption;
import org.dcache.pool.assumption.PerformanceCostAssumption;
import org.dcache.vehicles.FileAttributes;

/**
 * Partition that implements the probabilistic weighted available space selection (WASS) algorithm.
 * <p>
 * Experimental. Details will likely change. At the moment only pools to which data is written are
 * selected according to the WASS algorithm. For reads the classic selection algorithm is used.
 */
public class WassPartition extends ClassicPartition {

    static final String TYPE = "wass";

    private static final long serialVersionUID = -3587599095801229561L;

    private final WeightedAvailableSpaceSelection wass;

    public WassPartition() {
        this(NO_PROPERTIES);
    }

    public WassPartition(Map<String, String> inherited) {
        this(inherited, NO_PROPERTIES);
    }

    protected WassPartition(Map<String, String> inherited,
          Map<String, String> properties) {
        super(inherited, properties);
        wass = new WeightedAvailableSpaceSelection(_performanceCostFactor, _spaceCostFactor);
    }

    @Override
    protected Partition create(Map<String, String> inherited,
          Map<String, String> properties) {
        return new WassPartition(inherited, properties);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SelectedPool selectWritePool(CostModule cm,
          List<PoolInfo> pools,
          FileAttributes attributes,
          long preallocated)
          throws CacheException {
        PoolInfo pool = wass.selectByAvailableSpace(pools, preallocated, PoolInfo::getCostInfo);
        if (pool == null) {
            throw new CostException("All pools are full", null, _fallbackOnSpace, false);
        }
        return new SelectedPool(pool, new AvailableSpaceAssumption(preallocated));
    }

    /* REVISIT: The current implementation is a mix of the read pool
     * selection from ClassicPartition and write pool selection using
     * WASS. Code can probably be shared with ClassicPartition, but
     * since I hope to refine read pool selection, any code
     * duplication should be temporary and it is not worth refactoring
     * ClassicPartition.
     */
    @Override
    public P2pPair selectPool2Pool(CostModule cm,
          List<PoolInfo> src,
          List<PoolInfo> dst,
          FileAttributes attributes,
          boolean force)
          throws CacheException {
        checkState(!src.isEmpty());
        checkState(!dst.isEmpty());

        /* The maximum number of replicas can be limited.
         */
        if (src.size() >= _maxPnfsFileCopies) {
            throw new PermissionDeniedCacheException(
                  "P2P denied: already too many copies (" + src.size() + ")");
        }

        /* Randomise order of pools with equal cost. In particular
         * important when cost factors are 0.
         */
        Collections.shuffle(src);

        /* Source pools are only selected by performance cost, because
         * we will only read from the pool
         */
        List<PoolCost> sources =
              src.stream().map(WassPartition::toPoolCost).sorted(_byPerformanceCost)
                    .collect(toList());
        if (!force && isAlertCostExceeded(sources.get(0).performanceCost)) {
            throw new SourceCostException(
                  "P2P denied: All source pools are too busy (performance cost > " + _alertCostCut
                        + ")");
        }

        /* The target pool must be below specified cost limits;
         * otherwise we wouldn't be able to read the file afterwards
         * without triggering another p2p.
         */
        double maxTargetCost =
              (_slope > 0.01)
                    ? _slope * sources.get(0).performanceCost
                    : getCurrentCostCut(cm);
        if (!force && maxTargetCost > 0.0) {
            dst = dst.stream().filter(pool -> toPoolCost(pool).performanceCost < maxTargetCost)
                  .collect(toList());
        }

        if (dst.isEmpty()) {
            throw new DestinationCostException(
                  "P2P denied: All destination pools are too busy (performance cost > "
                        + maxTargetCost + ")");
        }

        long filesize = attributes.getSize();
        Assumption sourceAssumption =
              force ? Assumptions.none() : PerformanceCostAssumption.of(_error, _alertCostCut);
        Assumption destinationAssumption = force
              ? new AvailableSpaceAssumption(filesize)
              : new AvailableSpaceAssumption(filesize).and(PerformanceCostAssumption.of(
                    _error, maxTargetCost));

        if (_allowSameHostCopy != SameHost.NOTCHECKED) {
            /* Loop over all sources and find the most appropriate
             * destination such that same host constraints are
             * satisfied.
             */
            for (PoolCost source : sources) {
                List<PoolInfo> destinations;
                if (source.host == null) {
                    destinations = dst;
                } else {
                    destinations = dst.stream().filter(d -> !d.getHostName().equals(source.host))
                          .collect(toList());
                }

                PoolInfo destination =
                      wass.selectByAvailableSpace(destinations, filesize, PoolInfo::getCostInfo);
                if (destination != null) {
                    return new P2pPair(new SelectedPool(source.pool, sourceAssumption),
                          new SelectedPool(destination, destinationAssumption));
                }
            }

            /* We could not find a pair on different hosts, what now?
             */
            if (_allowSameHostCopy == SameHost.NEVER) {
                throw new PermissionDeniedCacheException(
                      "P2P denied: sameHostCopy is 'never' and no matching pool found");
            }
        }

        PoolInfo destination = wass.selectByAvailableSpace(dst, filesize, PoolInfo::getCostInfo);
        if (destination == null) {
            throw new DestinationCostException("All pools are full");
        }
        return new P2pPair(new SelectedPool(sources.get(0).pool, sourceAssumption),
              new SelectedPool(destination, destinationAssumption));
    }

    private PoolInfo selectByPrevious(List<PoolInfo> pools,
          Optional<PoolInfo> previous,
          FileAttributes attributes) {
        if (previous.isPresent() && _allowSameHostRetry != SameHost.NOTCHECKED) {
            List<PoolInfo> filteredPools = pools.stream()
                  .filter(p -> !Objects.equals(p.getHostName(), previous.get().getHostName())
                        && !Objects.equals(p.getName(), previous.get().getName()))
                  .collect(toList());
            PoolInfo pool = wass.selectByAvailableSpace(filteredPools, attributes.getSize(),
                  PoolInfo::getCostInfo);
            if (pool != null) {
                return pool;
            }
            if (_allowSameHostRetry == SameHost.NEVER) {
                return null;
            }
        }

        if (previous.isPresent()) {
            List<PoolInfo> filteredPools = pools.stream()
                  .filter(p -> !Objects.equals(p.getName(), previous.get().getName()))
                  .collect(toList());
            PoolInfo pool = wass.selectByAvailableSpace(filteredPools, attributes.getSize(),
                  PoolInfo::getCostInfo);
            if (pool != null) {
                return pool;
            }
        }

        return wass.selectByAvailableSpace(pools, attributes.getSize(), PoolInfo::getCostInfo);
    }

    @Override
    public SelectedPool selectStagePool(CostModule cm,
          List<PoolInfo> pools,
          Optional<PoolInfo> previous,
          FileAttributes attributes)
          throws CacheException {
        if (_fallbackCostCut > 0.0) {
            /* Filter by fallback cost; ensures that the file does not
             * get staged to a pool from which cost prevents us from
             * reading it.
             */
            List<PoolInfo> filtered =
                  pools.stream().filter(pool -> toPoolCost(pool).performanceCost < _fallbackCostCut)
                        .collect(toList());
            PoolInfo pool =
                  selectByPrevious(filtered, previous, attributes);
            if (pool != null) {
                return new SelectedPool(pool,
                      PerformanceCostAssumption.of(_error, _fallbackCostCut));
            }

            /* Didn't find a pool. Redo the selection from the full
             * set, but signal that the caller should fall back to
             * other links if possible.
             */
            pool = selectByPrevious(pools, previous, attributes);
            if (pool == null) {
                throw new CostException("All pools full",
                      null, true, false);
            } else {
                throw new CostException("Fallback cost exceeded",
                      new SelectedPool(pool), true, false);
            }
        } else {
            PoolInfo pool =
                  selectByPrevious(pools, previous, attributes);
            if (pool == null) {
                throw new CostException("All pools full",
                      null, true, false);
            }
            return new SelectedPool(pool);
        }
    }
}
