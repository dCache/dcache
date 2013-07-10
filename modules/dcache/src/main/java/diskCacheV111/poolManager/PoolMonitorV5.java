//  $Id: PoolMonitorV5.java,v 1.32 2007-08-01 20:00:45 tigran Exp $

package diskCacheV111.poolManager;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CostException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.FileNotOnlineCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellMessage;

import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.PartitionManager;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.PoolSelector;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Predicates.*;
import static com.google.common.collect.Iterables.*;
import static org.dcache.namespace.FileAttribute.*;

public class PoolMonitorV5
    implements Serializable, PoolMonitor
{
    private final static Logger _log =
        LoggerFactory.getLogger(PoolMonitorV5.class);

    private static final long serialVersionUID = -2400834413958127412L;

    private PoolSelectionUnit _selectionUnit ;
    private CostModule        _costModule    ;
    private PartitionManager  _partitionManager ;

    @Override
    public PoolSelectionUnit getPoolSelectionUnit()
    {
        return _selectionUnit;
    }

    public void setPoolSelectionUnit(PoolSelectionUnit selectionUnit)
    {
        _selectionUnit = selectionUnit;
    }

    @Override
    public CostModule getCostModule()
    {
        return _costModule;
    }

    public void setCostModule(CostModule costModule)
    {
        _costModule = costModule;
    }

    @Override
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

    @Override
    public PoolSelector getPoolSelector(FileAttributes fileAttributes,
                                        ProtocolInfo protocolInfo,
                                        String linkGroup)
    {
        return new PnfsFileLocation(fileAttributes, protocolInfo, linkGroup);
    }

    public class PnfsFileLocation implements PoolSelector
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


        @Override
        public Partition getCurrentPartition()
        {
            return _partition;
        }

        /**
         * Returns the result of a PSU match for this PnfsFileLocation.
         */
        private PoolPreferenceLevel[] match(DirectionType direction)
        {
            String hostName =
                (_protocolInfo instanceof IpProtocolInfo)
                ? ((IpProtocolInfo) _protocolInfo).getSocketAddress().getAddress().getHostAddress()
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

        @Override
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

        @Override
        public PoolInfo selectWritePool(long preallocated)
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
                    return partition.selectWritePool(_costModule, pools, _fileAttributes, preallocated);
                }
            }

            throw new CacheException(20,
                                     "No write pool available for <" +  _fileAttributes.getStorageInfo() +
                                     "> in the linkGroup " +
                                     (_linkGroup == null ? "[none]" : _linkGroup));
        }

        @Override
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
                List<PoolInfo> pools = new ArrayList<>(poolNames.size());
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

        @Override
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

        @Override
        public PoolInfo selectStagePool(String previousPool,
                                        String previousHost)
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
        @Override
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

    @Override
    public Collection<PoolCostInfo>
        queryPoolsByLinkName(String linkName)
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
        return queryPoolsForCost(transform(link.getPools(), getName));
    }

    private List<PoolCostInfo> queryPoolsForCost(Iterable<String> pools)
    {
        List<PoolCostInfo> list = new ArrayList<>();
        for( String poolName: pools ){
            PoolCostInfo cost = _costModule.getPoolCostInfo(poolName);
            if (cost != null) {
               list.add(cost);
            }
        }
        return list ;
    }

    @Nullable
    private PoolManagerPoolInformation getPoolInformation(@Nonnull PoolSelectionUnit.SelectionPool pool)
    {
        String name = pool.getName();
        PoolCostInfo cost = _costModule.getPoolCostInfo(name);
        if (!pool.isActive() || cost == null) {
            return null;
        }
        return new PoolManagerPoolInformation(name, cost);
    }

    @Nonnull
    private Collection<PoolManagerPoolInformation>
        getPoolInformation(@Nonnull Collection<PoolSelectionUnit.SelectionPool> pools)
    {
        List<PoolManagerPoolInformation> result = new ArrayList<>();
        for (PoolSelectionUnit.SelectionPool pool: pools) {
            PoolManagerPoolInformation poolInformation = getPoolInformation(pool);
            if (poolInformation != null) {
                result.add(poolInformation);
            }
        }
        return result;
    }

    @Override @Nullable
    public PoolManagerPoolInformation getPoolInformation(@Nonnull String name)
    {
        PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(name);
        return (pool == null) ? null : getPoolInformation(pool);
    }

    @Override @Nonnull
    public Collection<PoolManagerPoolInformation>
        getPoolsByLink(@Nonnull String linkName)
        throws NoSuchElementException
    {
        PoolSelectionUnit.SelectionLink link =
            _selectionUnit.getLinkByName(linkName);
        return new ArrayList<>(getPoolInformation(link.getPools()));
    }

    @Override @Nonnull
    public Collection<PoolManagerPoolInformation>
        getPoolsByPoolGroup(@Nonnull String poolGroup)
        throws NoSuchElementException
    {
        Collection<PoolSelectionUnit.SelectionPool> pools =
            _selectionUnit.getPoolsByPoolGroup(poolGroup);
        return new ArrayList<>(getPoolInformation(pools));
    }

    public static Set<FileAttribute> getRequiredAttributesForFileLocality()
    {
        return EnumSet.of(STORAGEINFO, SIZE, LOCATIONS);
    }

    @Override
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
