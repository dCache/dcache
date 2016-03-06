//  $Id: PoolMonitorV5.java,v 1.32 2007-08-01 20:00:45 tigran Exp $

package diskCacheV111.poolManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CostException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.FileNotOnlineCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellMessage;

import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.PartitionManager;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolSelector;
import org.dcache.poolmanager.SerializablePoolMonitor;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Strings.nullToEmpty;
import static java.util.stream.Collectors.toList;
import static org.dcache.namespace.FileAttribute.*;

public class PoolMonitorV5
    extends SerializablePoolMonitor
{
    private static final Logger _log =
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
            String hostName = getHostName();
            String protocol = getProtocol();
            return _selectionUnit.match(direction,
                                        hostName,
                                        protocol,
                                        _fileAttributes,
                                        _linkGroup);
        }

        @Override
        public List<List<PoolInfo>> getReadPools()
        {
            Map<String,PoolInfo> onlineLocations = _costModule.getPoolInfoAsMap(_fileAttributes.getLocations());
            return Stream
                    .of(match(DirectionType.READ))
                    .map(level -> level.getPoolList().stream().map(onlineLocations::get).filter(Objects::nonNull).collect(toList()))
                    .filter(levels -> !levels.isEmpty())
                    .collect(toList());
        }

        @Override
        public PoolInfo selectWritePool(long preallocated)
            throws CacheException
        {
            String hostName = getHostName();
            String protocol = getProtocol();
            PoolPreferenceLevel[] levels = _selectionUnit.match(DirectionType.WRITE,
                    hostName,
                    protocol,
                    _fileAttributes,
                    _linkGroup);

            if (levels.length == 0) {
                throw new CacheException(CacheException.NO_POOL_CONFIGURED,
                                         "No write links configured for [" +
                                                 "net=" + hostName +
                                                 ",protocol=" + protocol +
                                                 ",store=" + _fileAttributes.getStorageClass() + "@" + _fileAttributes.getHsm() +
                                                 ",cache=" + nullToEmpty(_fileAttributes.getCacheClass()) +
                                                 ",linkgroup=" + nullToEmpty(_linkGroup) + "]");
            }

            CostException fallback = null;
            for (PoolPreferenceLevel level: levels) {
                List<PoolInfo> pools =
                        level.getPoolList().stream()
                                .map(_costModule::getPoolInfo)
                                .filter(Objects::nonNull)
                                .collect(toList());
                if (!pools.isEmpty()) {
                    Partition partition = _partitionManager.getPartition(level.getTag());
                    try {
                        return partition.selectWritePool(_costModule, pools, _fileAttributes, preallocated);
                    } catch (CostException e) {
                        if (!e.shouldFallBack()) {
                            throw e;
                        }
                        fallback = e;
                    }
                }
            }

            /* We were asked to fall back, but all available links were
             * exhausted. Let the caller deal with it.
             */
            if (fallback != null) {
                throw fallback;
            }

            throw new CacheException(CacheException.NO_POOL_ONLINE,
                                     "No write pools online for [" +
                                             "net=" + hostName +
                                             ",protocol=" + protocol +
                                             ",store=" + _fileAttributes.getStorageClass() + "@" + _fileAttributes.getHsm() +
                                             ",cache=" + nullToEmpty(_fileAttributes.getCacheClass()) +
                                             ",linkgroup=" + nullToEmpty(_linkGroup) + "]");
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
            String hostName = getHostName();
            String protocol = getProtocol();
            PoolPreferenceLevel[] level = _selectionUnit.match(DirectionType.READ,
                    hostName,
                    protocol,
                    _fileAttributes,
                    _linkGroup);

            /* An empty array indicates that no links were found that
             * could serve the request. No reason to try any further;
             * not even a stage or P2P would help.
             */
            if (level.length == 0) {
                throw new CacheException(CacheException.NO_POOL_CONFIGURED, "No read links configured [" +
                        "net=" + hostName +
                        ",protocol=" + protocol +
                        ",store=" + _fileAttributes.getStorageClass() + "@" + _fileAttributes.getHsm() +
                        ",cache=" + nullToEmpty(_fileAttributes.getCacheClass()) +
                        ",linkgroup=" + nullToEmpty(_linkGroup) + "]");
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

        private String getHostName() {
            return (_protocolInfo instanceof IpProtocolInfo)
            ? ((IpProtocolInfo) _protocolInfo).getSocketAddress().getAddress().getHostAddress()
            : null;
        }

        private String getProtocol() {
            return _protocolInfo.getProtocol() + "/" +
            _protocolInfo.getMajorVersion();
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
                        level.getPoolList().stream()
                                .filter(pool -> !sources.containsKey(pool))
                                .map(_costModule::getPoolInfo)
                                .filter(Objects::nonNull)
                                .collect(toList());
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
                            level.getPoolList().stream()
                                    .filter(pool -> !locations.contains(pool))
                                    .map(_costModule::getPoolInfo)
                                    .filter(Objects::nonNull)
                                    .collect(toList());
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
            String hostName = getHostName();
            String protocol = getProtocol();
            PoolPreferenceLevel[] levels = _selectionUnit.match(DirectionType.READ,
                                                                hostName,
                                                                protocol,
                                                                _fileAttributes,
                                                                _linkGroup);
            for (PoolPreferenceLevel level: levels) {
                List<String> pools = level.getPoolList();
                if (!pools.isEmpty()) {
                    /* Now we know that the file could be pinned/read if
                     * any of these pools have the file.
                     */
                    isRequestSatisfiable = true;

                    Optional<PoolInfo> pool =
                            pools.stream().filter(onlinePools::containsKey).min(ordering).map(onlinePools::get);
                    if (pool.isPresent()) {
                        return pool.get();
                    }
                }
            }

            if (levels.length == 0) {
                throw new CacheException(CacheException.NO_POOL_CONFIGURED, "No read links configured [" +
                                                                            "net=" + hostName +
                                                                            ",protocol=" + protocol +
                                                                            ",store=" + _fileAttributes.getStorageClass() + "@" + _fileAttributes.getHsm() +
                                                                            ",cache=" + nullToEmpty(_fileAttributes.getCacheClass()) +
                                                                            ",linkgroup=" + nullToEmpty(_linkGroup) + "]");
            } else if (!isRequestSatisfiable) {
                throw new CacheException(CacheException.NO_POOL_ONLINE,
                                         "No read pools online for [" +
                                         "net=" + hostName +
                                         ",protocol=" + protocol +
                                         ",store=" + _fileAttributes.getStorageClass() + "@" + _fileAttributes.getHsm() +
                                         ",cache=" + nullToEmpty(_fileAttributes.getCacheClass()) +
                                         ",linkgroup=" + nullToEmpty(_linkGroup) + "]");
            } else if (onlinePools.isEmpty() && !_fileAttributes.getStorageInfo().isStored()) {
                throw new FileNotInCacheException("File is unavailable.");
            } else {
                throw new FileNotOnlineCacheException("File is nearline.");
            }
        }
    }

    @Override
    public Collection<PoolCostInfo> queryPoolsByLinkName(String linkName)
    {
        return _selectionUnit
                .getLinkByName(linkName)
                .getPools()
                .stream()
                .map(PoolSelectionUnit.SelectionEntity::getName)
                .map(_costModule::getPoolCostInfo)
                .filter(Objects::nonNull)
                .collect(toList());
    }

    public static Set<FileAttribute> getRequiredAttributesForFileLocality()
    {
        return EnumSet.of(STORAGEINFO, SIZE, LOCATIONS);
    }

    @Override
    public FileLocality
        getFileLocality(FileAttributes attributes, String hostName)
    {
        if (attributes.getFileType() == FileType.DIR || !attributes.isDefined(SIZE)) {
            return FileLocality.NONE;
        }

        PoolPreferenceLevel[] levels =
            _selectionUnit.match(DirectionType.READ,
                                 hostName,
                                 "*/*",
                                 attributes,
                                 null);

        Collection<String> locations = attributes.getLocations();
        for (PoolPreferenceLevel level: levels) {
            if (!Collections.disjoint(level.getPoolList(), locations)) {
                return (attributes.getStorageInfo().isStored()
                        ? FileLocality.ONLINE_AND_NEARLINE
                        : FileLocality.ONLINE);
            }
        }

        if (attributes.getStorageInfo().isStored()) {
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
