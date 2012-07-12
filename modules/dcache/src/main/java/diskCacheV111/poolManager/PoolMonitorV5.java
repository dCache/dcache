//  $Id: PoolMonitorV5.java,v 1.32 2007-08-01 20:00:45 tigran Exp $

package diskCacheV111.poolManager;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.primitives.Ints;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterables.isEmpty;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.EnumSet;
import java.util.NoSuchElementException;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.CostException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.FileNotOnlineCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.PoolCheckable;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.pools.PoolCostInfo;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileType;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.PartitionManager;
import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.ClassicPartition;
import org.dcache.poolmanager.PoolInfo;
import static org.dcache.namespace.FileAttribute.*;
import dmg.cells.nucleus.CellMessage;

public class PoolMonitorV5
    implements Serializable
{
    private final static Logger _log =
        LoggerFactory.getLogger(PoolMonitorV5.class);

    static final long serialVersionUID = -2400834413958127412L;

    private PoolSelectionUnit _selectionUnit ;
    private CostModule        _costModule    ;
    private PartitionManager  _partitionManager ;

    public PoolSelectionUnit getPoolSelectionUnit()
    {
        return _selectionUnit;
    }

    public void setPoolSelectionUnit(PoolSelectionUnit selectionUnit)
    {
        _selectionUnit = selectionUnit;
    }

    public CostModule getCostModule()
    {
        return _costModule;
    }

    public void setCostModule(CostModule costModule)
    {
        _costModule = costModule;
    }

    public PartitionManager getPartitionManager()
    {
        return _partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager)
    {
        _partitionManager = partitionManager;
    }

    public void messageToCostModule(CellMessage cellMessage)
    {
        _costModule.messageArrived(cellMessage);
    }

    public PnfsFileLocation getPnfsFileLocation(FileAttributes fileAttributes,
                                                ProtocolInfo protocolInfo,
                                                String linkGroup)
    {
        return new PnfsFileLocation(fileAttributes, protocolInfo, linkGroup);
    }

    public class PnfsFileLocation
    {
        private Partition _partition;

        private final FileAttributes _fileAttributes;
        private final ProtocolInfo _protocolInfo;
        private final String _linkGroup;

        public PnfsFileLocation(FileAttributes fileAttributes,
                                ProtocolInfo protocolInfo,
                                String linkGroup)
        {
            _fileAttributes = fileAttributes;
            _protocolInfo = protocolInfo;
            _linkGroup    = linkGroup;
        }

        /**
         * Returns the partition used for the last result of
         * selectReadPool.
         */
        public Partition getCurrentParameterSet()
        {
            return _partition;
        }

        /**
         * Returns the result of a PSU match for this
         * PnfsFileLocation.
         */
        public PoolPreferenceLevel[] match(DirectionType direction)
        {
            String hostName =
                (_protocolInfo instanceof IpProtocolInfo)
                ? ((IpProtocolInfo) _protocolInfo).getHosts()[0]
                : null;
            String protocol =
                _protocolInfo.getProtocol() + "/" +
                _protocolInfo.getMajorVersion() ;
            return _selectionUnit.match(direction,
                                        hostName,
                                        protocol,
                                        _fileAttributes.getStorageInfo(),
                                        _linkGroup);
        }

        /**
         * Returns all available read pools for this
         * PnfsFileLocation. Read pools are grouped and ordered
         * according to link preferences.
         */
        public List<List<PoolInfo>> getReadPools()
        {
            Map<String,PoolInfo> onlineLocations =
                _costModule.getPoolInfoAsMap(_fileAttributes.getLocations());
            Function<String,PoolInfo> toPoolInfo =
                Functions.forMap(onlineLocations, null);

            List<List<PoolInfo>> result = Lists.newArrayList();
            for (PoolPreferenceLevel level: match(DirectionType.READ)) {
                List<PoolInfo> pools =
                    Lists.newArrayList(filter(transform(level.getPoolList(),
                                                        toPoolInfo),
                                              notNull()));
                if (!pools.isEmpty()) {
                    result.add(pools);
                }
            }
            return result;
        }

        /**
         * Returns a pool for writing a file of the given size
         * described by this PnfsFileLocation.
         */
        public PoolInfo selectWritePool()
            throws CacheException
        {
            PoolPreferenceLevel[] levels = match(DirectionType.WRITE);

            if (levels.length == 0) {
                throw new CacheException(19,
                                         "No write pools configured for <" +
                                         _fileAttributes.getStorageInfo() +
                                         "> in the linkGroup " +
                                         (_linkGroup == null ? "[none]" : _linkGroup));
            }

            for (PoolPreferenceLevel level: levels) {
                List<PoolInfo> pools =
                    _costModule.getPoolInfo(level.getPoolList());
                if (!pools.isEmpty()) {
                    Partition partition =
                        _partitionManager.getPartition(level.getTag());
                    return partition.selectWritePool(_costModule, pools, _fileAttributes);
                }
            }

            throw new CacheException(20,
                                     "No write pool available for <" +  _fileAttributes.getStorageInfo() +
                                     "> in the linkGroup " +
                                     (_linkGroup == null ? "[none]" : _linkGroup));
        }

        /**
         * Returns a pool for reading the file.
         *
         * The partition used for the pool selection is available after
         * this method returns by calling getCurrentParameterSet().
         *
         * @throw FileNotInCacheException if the file is not on any
         *        pool that is online.
         * @throw PermissionDeniedCacheException if the file is is not
         *        on a pool from which we are allowed to read it
         * @throw CostExceededException if a read pool is available, but
         *        it exceed cost limits; the exception contains information
         *        about how the caller may recover
         * @throw
         */
        public PoolInfo selectReadPool()
            throws CacheException
        {
            Collection<String> locations = _fileAttributes.getLocations();
            _log.debug("[read] Expected from pnfs: {}", locations);

            Map<String,PoolInfo> onlinePools =
                _costModule.getPoolInfoAsMap(locations);
            _log.debug("[read] Online pools: {}", onlinePools);

            /* Is the file in any of the online pools?
             */
            if (onlinePools.isEmpty()){
                throw new FileNotInCacheException("File not in any pool");
            }

            /* Get the prioritized list of allowed pools for this
             * request.
             */
            PoolPreferenceLevel[] level = match(DirectionType.READ);

            /* An empty array indicates that no links were found that
             * could serve the request. No reason to try any further;
             * not even a stage or P2P would help.
             */
            if (level.length == 0) {
                throw new CacheException("No links for this request");
            }

            CostException fallback = null;
            for (int prio = 0; prio < level.length; prio++) {
                List<String> poolNames = level[prio].getPoolList();
                _log.debug("[read] Allowed pools at level {}: {}",
                           prio, poolNames);

                /* Reduce the set to the pools that are supposed to
                 * contain the file and which are online.
                 */
                List<PoolInfo> pools = new ArrayList<PoolInfo>(poolNames.size());
                for (String poolName: poolNames) {
                    PoolInfo info = onlinePools.get(poolName);
                    if (info != null) {
                        pools.add(info);
                    }
                }
                _log.debug("[read] Available pools at level {}: {}",
                           prio, pools);

                /* The caller may want to know which partition we used
                 * to select a pool.
                 */
                _partition =
                    _partitionManager.getPartition(level[prio].getTag());

                /* Fallback to next link if current link doesn't point
                 * to any available pools.
                 */
                if (pools.isEmpty()) {
                    continue;
                }

                /* The actual pool selection is delegated to the
                 * Partition.
                 */
                try {
                    return _partition.selectReadPool(_costModule, pools,
                                                     _fileAttributes);
                } catch (CostException e) {
                    if (!e.shouldFallBack()) {
                        throw e;
                    }
                    fallback = e;
                }
            }

            /* We were asked to fall back, but all available links were
             * exhausted. Let the caller deal with it.
             */
            if (fallback != null) {
                throw fallback;
            }

            /* None of the pools we were allowed to read from were
             * online or had the file.
             */
            throw new PermissionDeniedCacheException("File is online, but not in read-allowed pool");
        }

        public Partition.P2pPair selectPool2Pool(boolean force)
            throws CacheException
        {
            Collection<String> locations = _fileAttributes.getLocations();
            _log.debug("[p2p] Expected source from pnfs: {}", locations);
            Map<String,PoolInfo> sources =
                _costModule.getPoolInfoAsMap(locations);
            _log.debug("[p2p] Online source pools: {}", sources.values());

            if (sources.size() == 0) {
                throw new CacheException("P2P denied: No source pools available");
            }

            PoolPreferenceLevel[] levels = match(DirectionType.P2P);
            for (PoolPreferenceLevel level: levels) {
                List<PoolInfo> pools =
                    _costModule.getPoolInfo(filter(level.getPoolList(),
                                                   not(in(sources.keySet()))));
                if (!pools.isEmpty()) {
                    _log.debug("[p2p] Online destination candidates: {}", pools);
                    Partition partition =
                        _partitionManager.getPartition(level.getTag());
                    return partition.selectPool2Pool(_costModule,
                                                     Lists.newArrayList(sources.values()),
                                                     pools,
                                                     _fileAttributes,
                                                     force);
                }
            }

            throw new PermissionDeniedCacheException("P2P denied: No pool candidates available/configured/left for p2p or file already everywhere");
        }

        public PoolInfo selectStagePool(String previousPool, String previousHost)
            throws CacheException
        {
            Collection<String> locations = _fileAttributes.getLocations();
            _log.debug("[stage] Existing locations of the file: {}", locations);

            CostException fallback = null;
            for (PoolPreferenceLevel level: match(DirectionType.CACHE)) {
                try {
                    List<PoolInfo> pools =
                        _costModule.getPoolInfo(filter(level.getPoolList(),
                                                       not(in(locations))));
                    if (!pools.isEmpty()) {
                        _log.debug("[stage] Online stage candidates: {}", pools);
                        Partition partition =
                            _partitionManager.getPartition(level.getTag());
                        return partition.selectStagePool(_costModule, pools,
                                                         previousPool,
                                                         previousHost,
                                                         _fileAttributes);
                    }
                } catch (CostException e) {
                    if (!e.shouldFallBack()) {
                        throw e;
                    }
                    fallback = e;
                }
            }

            /* We were asked to fall back, but all available links were
             * exhausted. Let the caller deal with it.
             */
            if (fallback != null) {
                throw fallback;
            }

            throw new CacheException(149, "No pool candidates available/configured/left for stage");
        }

        // FIXME: There is a fair amount of overlap between this method
        // and getFileLocality.
        public PoolInfo selectPinPool()
            throws CacheException
        {
            /* This is the same arbitrary but deterministic ordering we
             * use for min cost cut handling.
             */
            Ordering<String> ordering =
                new Ordering<String>()
                {
                    final String id = _fileAttributes.getPnfsId().toString();

                    @Override
                    public int compare(String pool1, String pool2)
                    {
                        String s1 = id + pool1;
                        String s2 = id + pool2;
                        return Ints.compare(s1.hashCode(), s2.hashCode());
                    }
                };

            Collection<String> locations = _fileAttributes.getLocations();
            _log.debug("[pin] Expected from pnfs: {}", locations);

            Map<String,PoolInfo> onlinePools =
                _costModule.getPoolInfoAsMap(locations);
            _log.debug("[pin] Online pools: {}", onlinePools.values());

            boolean isRequestSatisfiable = false;
            for (PoolPreferenceLevel level: match(DirectionType.READ)) {
                List<String> pools = level.getPoolList();
                if (!pools.isEmpty()) {
                    /* Now we know that the file could be pinned/read if
                     * any of these pools have the file.
                     */
                    isRequestSatisfiable = true;
                    Iterable<String> pinnablePools =
                        filter(pools, in(onlinePools.keySet()));
                    if (!isEmpty(pinnablePools)) {
                        return onlinePools.get(ordering.min(pinnablePools));
                    }
                }
            }

            if (isRequestSatisfiable &&
                (!onlinePools.isEmpty() || _fileAttributes.getStorageInfo().isStored())) {
                throw new FileNotOnlineCacheException("File is not on online");
            } else {
                throw new FileNotInCacheException("File is unavailable");
            }
        }
    }

    public Collection<PoolCostCheckable>
        queryPoolsByLinkName(String linkName, long filesize)
    {
        Function<PoolSelectionUnit.SelectionPool,String> getName =
            new Function<PoolSelectionUnit.SelectionPool,String>() {
                @Override
                public String apply(PoolSelectionUnit.SelectionPool pool) {
                    return pool.getName();
                }
            };
        PoolSelectionUnit.SelectionLink link =
            _selectionUnit.getLinkByName(linkName);
        return queryPoolsForCost(transform(link.pools(), getName), filesize);
    }

    private List<PoolCostCheckable> queryPoolsForCost(Iterable<String> pools,
                                                      long filesize)
    {
        List<PoolCostCheckable> list = new ArrayList<PoolCostCheckable>();

        for( String poolName: pools ){

            PoolCostCheckable costCheck = _costModule.getPoolCost( poolName , filesize ) ;
            if( costCheck != null ){
               list.add( costCheck ) ;
               _log.info( "queryPoolsForCost : costModule : "+poolName+" ("+filesize+") "+costCheck);
            }
        }

        return list ;
    }

    private PoolManagerPoolInformation getPoolInformation(PoolSelectionUnit.SelectionPool pool)
    {
        String name = pool.getName();
        PoolManagerPoolInformation info = new PoolManagerPoolInformation(name);
        PoolCostCheckable cost = _costModule.getPoolCost(name, 0);
        if (!pool.isActive() || cost == null) {
            info.setSpaceCost(Double.POSITIVE_INFINITY);
            info.setCpuCost(Double.POSITIVE_INFINITY);
        } else {
            info.setSpaceCost(cost.getSpaceCost());
            info.setCpuCost(cost.getPerformanceCost());
        }
        info.setPoolCostInfo(_costModule.getPoolCostInfo(name));
        return info;
    }

    private Collection<PoolManagerPoolInformation>
        getPoolInformation(Collection<PoolSelectionUnit.SelectionPool> pools)
    {
        List<PoolManagerPoolInformation> result =
            new ArrayList<PoolManagerPoolInformation>();
        for (PoolSelectionUnit.SelectionPool pool: pools) {
            result.add(getPoolInformation(pool));
        }
        return result;
    }

    public PoolManagerPoolInformation getPoolInformation(String name)
        throws NoSuchElementException
    {
        PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(name);
        if (pool == null) {
            throw new NoSuchElementException("No such pool: " + name);
        }
        return getPoolInformation(pool);
    }

    public Collection<PoolManagerPoolInformation>
        getPoolsByLink(String linkName)
        throws NoSuchElementException
    {
        PoolSelectionUnit.SelectionLink link =
            _selectionUnit.getLinkByName(linkName);
        return new ArrayList(getPoolInformation(link.pools()));
    }

    public Collection<PoolManagerPoolInformation>
        getPoolsByPoolGroup(String poolGroup)
        throws NoSuchElementException
    {
        Collection<PoolSelectionUnit.SelectionPool> pools =
            _selectionUnit.getPoolsByPoolGroup(poolGroup);
        return new ArrayList(getPoolInformation(pools));
    }

    /**
     * Fetch the percentile performance cost; that is, the cost
     * of the <code>n</code>th pool, in increasing order of performance cost,
     * where <code>n</code> is <code>(int)floor( fraction * numberOfPools)</code>
     * @param fraction the percentile fraction.  The value must be between 0 and 1.
     * @return the nth percentile performance cost, or 0 if there are no pools.
     */
    public double getPoolsPercentilePerformanceCost( double fraction) {
        return _costModule.getPoolsPercentilePerformanceCost( fraction);
    }

    public static Set<FileAttribute> getRequiredAttributesForFileLocality()
    {
        return EnumSet.of(STORAGEINFO, SIZE, LOCATIONS);
    }

    public FileLocality
        getFileLocality(FileAttributes attributes, String hostName)
    {
        if (attributes.getFileType() == FileType.DIR ||
            attributes.getSize() == 0) {
            return FileLocality.NONE;
        }

        StorageInfo storageInfo = attributes.getStorageInfo();
        PoolPreferenceLevel[] levels =
            _selectionUnit.match(DirectionType.READ,
                                 hostName,
                                 "*/*",
                                 storageInfo,
                                 null);

        Collection<String> locations = attributes.getLocations();
        for (PoolPreferenceLevel level: levels) {
            if (!Collections.disjoint(level.getPoolList(), locations)) {
                return (storageInfo.isStored()
                        ? FileLocality.ONLINE_AND_NEARLINE
                        : FileLocality.ONLINE);
            }
        }

        if (storageInfo.isStored()) {
            return FileLocality.NEARLINE;
        }

        for (String name: locations) {
            PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(name);
            if (pool == null || !pool.canReadForP2P()) {
                continue;
            }

            PoolCostInfo cost = _costModule.getPoolCostInfo(name);
            if (cost == null) {
                continue;
            }

            // REVISIT: This check should be integrated into
            // SelectionPool.canReadForP2P
            if (cost.getP2pQueue().getMaxActive() > 0){
                return FileLocality.NEARLINE;
            }
        }
        return FileLocality.UNAVAILABLE;
    }
}
