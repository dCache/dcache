package diskCacheV111.poolManager;

import static diskCacheV111.poolManager.PoolSelectionUnit.UnitType.DCACHE;
import static diskCacheV111.poolManager.PoolSelectionUnit.UnitType.NET;
import static diskCacheV111.poolManager.PoolSelectionUnit.UnitType.PROTOCOL;
import static diskCacheV111.poolManager.PoolSelectionUnit.UnitType.STORE;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static org.dcache.namespace.FileAttribute.CACHECLASS;
import static org.dcache.namespace.FileAttribute.HSM;
import static org.dcache.namespace.FileAttribute.STORAGECLASS;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.CommandException;
import dmg.util.CommandSyntaxException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Args;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoolSelectionUnitV2
      implements Serializable, PoolSelectionUnit, PoolSelectionUnitAccess, CellSetupProvider,
      CellCommandListener, CellLifeCycleAware {

    private static final String __version = "$Id: PoolSelectionUnitV2.java,v 1.42 2007-10-25 14:03:54 tigran Exp $";
    private static final Logger LOGGER = LoggerFactory.getLogger(PoolSelectionUnitV2.class);
    private static final String NO_NET = "<no net>";
    private static final Set<FileAttribute> STORAGE_INFO = Set.of(FileAttribute.CACHECLASS,
          FileAttribute.STORAGECLASS, FileAttribute.HSM, FileAttribute.OWNER,
          FileAttribute.OWNER_GROUP);

    private static final String DEFAULT_PROTOCOL_UNIT = "*/*";
    private static final String DEFAULT_IPV4_NET_UNIT = "0.0.0.0/0.0.0.0";
    private static final String DEFAULT_IPV6_NET_UNIT = "::/0";

    @Override
    public String getVersion() {
        return __version;
    }

    private static final long serialVersionUID = 4852540784324544199L;

    private Collection<Pool> _poolsFromBeforeSetup;
    private final Map<String, PGroup> _pGroups = new HashMap<>();
    private final Map<String, Pool> _pools = new HashMap<>();
    private final Map<String, Link> _links = new HashMap<>();
    private final Map<String, LinkGroup> _linkGroups = new HashMap<>();
    private final Map<String, UGroup> _uGroups = new HashMap<>();
    private final Map<String, Unit> _units = new HashMap<>();
    private boolean _useRegex;
    private boolean _allPoolsActive;

    /**
     * Ok, this is the critical part of PoolManager, but (!!!) the whole select path is READ-ONLY,
     * unless we change setup. So ReadWriteLock is what we are looking for, while is a point of
     * serialization.
     */

    private final ReentrantReadWriteLock _psuReadWriteLock = new ReentrantReadWriteLock();
    private final Lock _psuReadLock = _psuReadWriteLock.readLock();
    private final Lock _psuWriteLock = _psuReadWriteLock.writeLock();

    private final NetHandler _netHandler = new NetHandler();

    private transient PnfsHandler _pnfsHandler;

    @Override
    public Map<String, SelectionLink> getLinks() {
        rlock();
        try {
            return Maps.newHashMap(_links);
        } finally {
            runlock();
        }
    }

    @Override
    public Map<String, SelectionUnit> getSelectionUnits() {
        rlock();
        try {
            return Maps.newHashMap(_units);
        } finally {
            runlock();
        }
    }

    @Override
    public Map<String, SelectionUnitGroup> getUnitGroups() {
        rlock();
        try {
            return Maps.newHashMap(_uGroups);
        } finally {
            runlock();
        }
    }

    @Override
    public Collection<SelectionLink> getLinksPointingToPoolGroup(String poolGroup)
          throws NoSuchElementException {
        rlock();
        try {
            PGroup group = _pGroups.get(poolGroup);
            if (group == null) {
                throw new NoSuchElementException("No such pool group: " + poolGroup);
            }
            return new ArrayList<>(group._linkList.values());
        } finally {
            runlock();
        }
    }

    @Override
    public SelectionLink getLinkByName(String name) throws NoSuchElementException {

        Link link = null;

        rlock();
        try {
            link = _links.get(name);
            if (link == null) {
                throw new NoSuchElementException("Link not found : " + name);
            }
        } finally {
            runlock();
        }
        return link;
    }

    @Override
    public String[] getDefinedPools(boolean enabledOnly) {

        rlock();
        try {
            return _pools.values().stream()
                  .filter(p -> p.isEnabled() || !enabledOnly)
                  .map(Pool::getName)
                  .toArray(String[]::new);
        } finally {
            runlock();
        }
    }

    @Override
    public String[] getActivePools() {

        rlock();
        try {
            return _pools.values().stream()
                  .filter(Pool::isActive)
                  .filter(Pool::isEnabled)
                  .map(Pool::getName)
                  .toArray(String[]::new);
        } finally {
            runlock();
        }
    }

    @Override
    public void beforeSetup() {
        wlock();
        _poolsFromBeforeSetup = Lists.newArrayList(_pools.values());
        clear();
    }

    @Override
    public void afterSetup() {
        _poolsFromBeforeSetup.stream().filter(Pool::isActive)
              .forEach(p -> {
                  Pool pool = _pools.get(p.getName());
                  if (pool == null) {
                      pool = new Pool(p.getName());
                      _pools.put(pool.getName(), pool);
                      PGroup group = _pGroups.get("default");
                      if (group != null) {
                          pool._pGroupList.put(group.getName(), group);
                          group._poolList.put(pool.getName(), pool);
                      }
                  }
                  pool.setAddress(p.getAddress());
                  pool.setPoolMode(p.getPoolMode());
                  pool.setHsmInstances(p.getHsmInstances());
                  pool.setActive(true);
                  pool.setSerialId(p.getSerialId());
              });
        _poolsFromBeforeSetup = null;
        wunlock();
    }

    @Override
    public void printSetup(PrintWriter pw) {
        rlock();
        try {
            pw.append("psu set regex ").println(_useRegex ? "on" : "off");
            pw.append("psu set allpoolsactive ").println(_allPoolsActive ? "on" : "off");
            pw.println();
            _units.values().stream().sorted(comparing(Unit::getType).thenComparing(Unit::getName))
                  .forEachOrdered(
                        unit -> {
                            pw.append("psu create unit ");
                            switch (unit.getType()) {
                                case STORE:
                                    pw.append("-store");
                                    break;
                                case DCACHE:
                                    pw.append("-dcache");
                                    break;
                                case PROTOCOL:
                                    pw.append("-protocol");
                                    break;
                                case NET:
                                    pw.append("-net");
                                    break;
                            }
                            pw.append(" ").println(unit.getName());

                            if (unit instanceof StorageUnit) {
                                StorageUnit sunit = (StorageUnit) unit;
                                Integer required = sunit.getRequiredCopies();
                                if (required != null) {
                                    pw.append("psu set storage unit ")
                                          .append(sunit.getName())
                                          .append(" -required=")
                                          .append(String.valueOf(required));
                                    List<String> tags = sunit.getOnlyOneCopyPer();
                                    if (!tags.isEmpty()) {
                                        pw.append(" -onlyOneCopyPer=")
                                              .append(Joiner.on(",").join(tags));
                                    }
                                    pw.println();
                                }
                            }
                        });
            pw.println();
            _uGroups.values().stream().sorted(comparing(UGroup::getName)).forEachOrdered(
                  group -> {
                      pw.append("psu create ugroup ").println(group.getName());
                      group._unitList.values().stream().sorted(comparing(Unit::getName))
                            .forEachOrdered(
                                  unit -> pw
                                        .append("psu addto ugroup ")
                                        .append(group.getName())
                                        .append(" ")
                                        .println(unit.getName()));
                      pw.println();
                  });
            _pools.values().stream().sorted(comparing(Pool::getName)).forEachOrdered(
                  pool -> {
                      pw.append("psu create pool ").append(pool.getName());
                      if (!pool.isPing()) {
                          pw.append(" -noping");
                      }
                      if (!pool.isEnabled()) {
                          pw.append(" -disabled");
                      }
                      if (pool.isReadOnly()) {
                          pw.append(" -rdonly");
                      }
                      pw.println();
                  });
            pw.println();

            _pGroups.values().stream().sorted(comparing(PGroup::getName)).forEachOrdered(
                  group -> {
                      pw.append("psu create pgroup ").append(group.getName());
                      if (group.isPrimary()) {
                          pw.append(" -resilient");
                      }
                      // don't explicitly add pools into dynamic pool groups
                      if (group instanceof DynamicPGroup) {
                          pw.append(" -dynamic");
                          Map<String, String> tags = ((DynamicPGroup) group).getTags();
                          String asOption = tags.entrySet().stream()
                                .map(e -> e.getKey() + "=" + e.getValue())
                                .collect(Collectors.joining(","));
                          pw.append(" -tags=" + asOption);
                          pw.println();
                      } else {
                          pw.println();
                          group._poolList.values().stream().sorted(comparing(Pool::getName))
                                .forEachOrdered(
                                      pool -> pw
                                            .append("psu addto pgroup ")
                                            .append(group.getName())
                                            .append(" ")
                                            .println(pool.getName())

                                );
                      }

                      pw.println();
                  });
            //nested -pools
            _pGroups.values().stream().sorted(comparing(PGroup::getName)).forEachOrdered(
                  group -> {
                      pw.println();
                      group._pgroupList.stream().sorted(comparing(PGroup::getName))
                            .forEachOrdered(
                                  groupS -> {pw
                                            .append("psu addto pgroup ")
                                            .append(group.getName())
                                            .append(" ")
                                            .println("@" + groupS.getName());
                                      LOGGER.info(groupS.getName() + " " + group.getName());
                                  }
                            );

                      pw.println();
                  });
            _links.values().stream().sorted(comparing(Link::getName)).forEachOrdered(
                  link -> {
                      pw.append("psu create link ").append(link.getName());
                      link._uGroupList.values().stream().map(UGroup::getName).sorted()
                            .forEachOrdered(
                                  name -> pw.append(" ").append(name));
                      pw.println();
                      pw.append("psu set link ").append(link.getName()).append(" ")
                            .println(link.getAttraction());
                      link._poolList.values().stream().sorted(comparing(PoolCore::getName))
                            .forEachOrdered(
                                  poolCore -> pw
                                        .append("psu addto link ")
                                        .append(link.getName())
                                        .append(" ")
                                        .println(poolCore.getName()));
                      pw.println();
                  });

            _linkGroups.values().stream().sorted(comparing(LinkGroup::getName)).forEachOrdered(
                  linkGroup -> {
                      pw.append("psu create linkGroup ").println(linkGroup.getName());

                      pw.append("psu set linkGroup custodialAllowed ").append(
                            linkGroup.getName()).append(" ").println(
                            linkGroup.isCustodialAllowed());
                      pw.append("psu set linkGroup replicaAllowed ").append(
                            linkGroup.getName()).append(" ").println(
                            linkGroup.isReplicaAllowed());
                      pw.append("psu set linkGroup nearlineAllowed ").append(
                            linkGroup.getName()).append(" ").println(
                            linkGroup.isNearlineAllowed());
                      pw.append("psu set linkGroup outputAllowed ").append(
                            linkGroup.getName()).append(" ").println(
                            linkGroup.isOutputAllowed());
                      pw.append("psu set linkGroup onlineAllowed ").append(
                            linkGroup.getName()).append(" ").println(
                            linkGroup.isOnlineAllowed());
                      linkGroup.getLinks().stream().sorted(comparing(SelectionLink::getName))
                            .forEachOrdered(
                                  link -> pw
                                        .append("psu addto linkGroup ")
                                        .append(linkGroup.getName())
                                        .append(" ")
                                        .println(link.getName()));
                      pw.println();
                  });
        } finally {
            runlock();
        }
    }

    public void clear() {
        wlock();
        try {
            _netHandler.clear();
            _pGroups.clear();
            _pools.clear();
            _links.clear();
            _uGroups.clear();
            _units.clear();
            _linkGroups.clear();
        } finally {
            wunlock();
        }
    }

    public void setActive(String poolName, boolean active) {
        wlock();
        try {
            Pool pool = _pools.get(poolName);
            if (pool != null) {
                pool.setActive(active);
            }
        } finally {
            wunlock();
        }
    }

    private int setEnabled(Glob glob, boolean enabled) {
        wlock();
        try {
            int count = 0;
            for (Pool pool : getPools(glob.toPattern())) {
                count++;
                pool.setEnabled(enabled);
            }
            return count;
        } finally {
            wunlock();
        }
    }

    @Override
    public SelectionPool getPool(String poolName) {

        SelectionPool pool = null;

        rlock();
        try {
            pool = _pools.get(poolName);
        } finally {
            runlock();
        }

        return pool;
    }

    @Override
    public boolean updatePool(String poolName, CellAddressCore address,
          String canonicalHostName, long serialId,
          PoolV2Mode mode, Set<String> hsmInstances,
          Map<String, String> tags) {
        /* For compatibility with previous versions of dCache, a pool
         * marked DISABLED, but without any other DISABLED_ flags set
         * is considered fully disabled.
         */
        boolean disabled =
              mode.getMode() == PoolV2Mode.DISABLED
                    || mode.isDisabled(PoolV2Mode.DISABLED_DEAD)
                    || mode.isDisabled(PoolV2Mode.DISABLED_STRICT);

        /* By convention, the serial number is set to zero when a pool
         * is disabled. This is used by the watchdog to identify, that
         * we have already announced that the pool is down.
         */
        long newSerialId = disabled ? 0 : serialId;

        /* The update is done in two steps; most of the time an update will not change anything except
         * refresh the heartbeat timestamp. We do this under a read lock.
         */
        rlock();
        try {
            Pool pool = _pools.get(poolName);
            if (pool != null) {
                /* Any change in the kind of operations a pool might be able
                 * to perform has to be propagated to a number of other
                 * components.
                 *
                 * Notice that calling setSerialId has a side-effect, which is
                 * why we call it first.
                 */
                boolean changed =
                      pool.getSerialId() != newSerialId
                            || pool.isActive() == disabled
                            || (mode.getMode() != pool.getPoolMode().getMode())
                            || !Objects.equals(pool.getHsmInstances(), hsmInstances)
                            || !Objects.equals(pool.getAddress(), address)
                            || !Objects.equals(pool.getCanonicalHostName(),
                            Optional.ofNullable(canonicalHostName));
                if (!changed) {
                    pool.setActive(!disabled);
                    return false;
                }
            }
        } finally {
            runlock();
        }

        /* We detected that something changed and fall through to a full update under a write lock.
         */
        wlock();
        try {
            Pool pool = _pools.get(poolName);
            if (pool == null) {
                pool = new Pool(poolName);
                _pools.put(pool.getName(), pool);
                PGroup group = _pGroups.get("default");
                if (group == null) {
                    throw new IllegalArgumentException("Not found : " + "default");
                }
                pool._pGroupList.put(group.getName(), group);
                group._poolList.put(pool.getName(), pool);
            }

            PoolV2Mode oldMode = pool.getPoolMode();

            /* Any change in the kind of operations a pool might be able
             * to perform has to be propagated to a number of other
             * components.
             *
             * Notice that calling setSerialId has a side-effect, which is
             * why we call it first.
             */
            boolean isRestarted = pool.setSerialId(newSerialId);
            boolean changed =
                  isRestarted
                        || pool.isActive() == disabled
                        || (mode.getMode() != oldMode.getMode())
                        || !Objects.equals(pool.getHsmInstances(), hsmInstances)
                        || !Objects.equals(pool.getAddress(), address)
                        || !Objects.equals(pool.getCanonicalHostName(),
                        Optional.ofNullable(canonicalHostName));

            if (mode.getMode() != oldMode.getMode()) {
                LOGGER.warn("Pool {} changed from mode {}  to {}.", poolName, oldMode, mode);
            }

            pool.setAddress(address);
            pool.setPoolMode(mode);
            pool.setHsmInstances(hsmInstances);
            pool.setActive(!disabled);
            pool.setCanonicalHostName(canonicalHostName);
            pool.setTags(tags);

            // create a dynamic pool group based on pool tags.
            if (isRestarted && !disabled) {
                final Pool p = pool;
                _pGroups.values().stream()
                      .filter(DynamicPGroup.class::isInstance)
                      .map(DynamicPGroup.class::cast)
                      .forEach(pg -> pg.addIfMatches(p));
            }

            return changed;
        } finally {
            wunlock();
        }
    }

    public Map<String, Link> match(Map<String, Link> map, Unit unit,
          DirectionType ioType) {

        Map<String, Link> newmap = match(unit, null, ioType);
        if (map == null) {
            return newmap;
        }

        Map<String, Link> resultMap = new HashMap<>();
        for (Link link : map.values()) {
            if (newmap.get(link.getName()) != null) {
                resultMap.put(link.getName(), link);
            }
        }
        return resultMap;
    }

    private LinkMap match(LinkMap linkMap, Unit unit, LinkGroup linkGroup,
          DirectionType ioType) {
        Map<String, Link> map = match(unit, linkGroup, ioType);
        for (Link link : map.values()) {
            linkMap.addLink(link);
        }
        return linkMap;
    }

    /**
     * Matches using logical AND.
     * <p>
     * The AND is only valid if we have at least as many units (from the arguments) as required by
     * the number of uGroupList(s).
     *
     * @param type             of I/O direction
     * @param netUnitName      IP address/hostname or <code>null</code> (= default)
     * @param protocolUnitName of the form name/version, or <code>null</code> (= default)
     * @param fileAttributes   with minimally storage info defined
     * @param linkGroupName    can be <code>null</code>
     * @param exclude          for matching excluded hosts
     * @return array of pools sorted by preference level
     */
    @Override
    public PoolPreferenceLevel[] match(DirectionType type, String netUnitName,
          String protocolUnitName,
          FileAttributes fileAttributes, String linkGroupName, Predicate<String> exclude) {
        requireNonNull(exclude, "Predicate argument cannot be null.");

        StorageInfo storageInfo = fileAttributes.getStorageInfo();
        String storageClass = storageInfo.getStorageClass();
        String hsm = storageInfo.getHsm();
        String dCacheUnitName = storageInfo.getCacheClass();

        /*
         *  The preference level build requires these to be present in the file attributes.
         */
        if (fileAttributes.isUndefined(STORAGECLASS)) {
            fileAttributes.setStorageClass(storageClass);
        }

        if (fileAttributes.isUndefined(HSM)) {
            fileAttributes.setHsm(hsm);
        }

        if (fileAttributes.isUndefined(CACHECLASS)) {
            fileAttributes.setCacheClass(dCacheUnitName);
        }

        String storeUnitName = storageClass + "@" + hsm;

        Map<String, String> variableMap = storageInfo.getMap();

        LOGGER.debug(
              "running match: type={} store={} dCacheUnit={} net={} protocol={} keys={} locations={} linkGroup={}",
              type, storeUnitName, dCacheUnitName, netUnitName, protocolUnitName,
              variableMap, storageInfo.locations(), linkGroupName);

        PoolPreferenceLevel[] result = null;
        rlock();
        try {
            List<Unit> units = new ArrayList<>();

            resolveStorageUnit(units, storeUnitName);
            addProtocolUnit(units, protocolUnitName);
            addDCacheUnit(units, dCacheUnitName);
            addNetUnit(units, netUnitName);

            LinkGroup linkGroup = resolveLinkGroup(linkGroupName);
            Set<Link> sortedSet = findMatchingLinks(units, linkGroup, type);

            List<List<Link>> linkLists = matchPreferences(type, sortedSet);
            result = buildPreferenceLevels(type, linkLists, fileAttributes, exclude);
        } finally {
            runlock();
        }

        if (LOGGER.isDebugEnabled()) {
            logResult(result);
        }

        return result;
    }

    private void resolveStorageUnit(List<Unit> list, String storeUnitName) {
        if (_useRegex) {
            Unit universalCoverage = null;
            Unit classCoverage = null;

            for (Unit unit : _units.values()) {
                if (unit.getType() != STORE) {
                    continue;
                }

                if (unit.getName().equals("*@*")) {
                    universalCoverage = unit;
                } else if (unit.getName().equals("*@" + storeUnitName)) {
                    classCoverage = unit;
                } else {
                    if (Pattern.matches(unit.getName(), storeUnitName)) {
                        list.add(unit);
                        break;
                    }
                }
            }

            if (list.isEmpty()) {
                if (classCoverage != null) {
                    list.add(classCoverage);
                } else if (universalCoverage != null) {
                    list.add(universalCoverage);
                } else {
                    throw new IllegalArgumentException(
                          "Unit not found : " + storeUnitName);
                }
            }
        } else {
            Unit unit = _units.get(storeUnitName);
            if (unit == null) {
                int ind = storeUnitName.lastIndexOf('@');
                if ((ind > 0) && (ind < (storeUnitName.length() - 1))) {
                    String template = "*@"
                          + storeUnitName.substring(ind + 1);
                    if ((unit = _units.get(template)) == null) {

                        if ((unit = _units.get("*@*")) == null) {
                            LOGGER.debug("no matching storage unit found for: {}",
                                  storeUnitName);
                            throw new IllegalArgumentException(
                                  "Unit not found : " + storeUnitName);
                        }
                    }
                } else {
                    throw new IllegalArgumentException(
                          "IllegalUnitFormat : " + storeUnitName);
                }
            }

            LOGGER.debug("matching storage unit found for: {}", storeUnitName);
            list.add(unit);
        }
    }

    private void addProtocolUnit(List<Unit> list, String protocolUnitName) {
        if (protocolUnitName != null) {
            Unit unit = findProtocolUnit(protocolUnitName);
            if (unit == null) {
                LOGGER.debug("no matching protocol unit found for: {}", protocolUnitName);
                /* for backward compatibility, do not throw exception */
                return;
            }

            LOGGER.debug("matching protocol unit found: {}", unit);
            list.add(unit);
        }
    }

    private void addDCacheUnit(List<Unit> list, String dCacheUnitName) {
        if (dCacheUnitName != null) {
            Unit unit = _units.get(dCacheUnitName);

            if (unit == null || unit.getType() != DCACHE) {
                LOGGER.debug("no matching dCache unit found for: {}", dCacheUnitName);
                throw new IllegalArgumentException("Unit not found : "
                      + dCacheUnitName);
            }

            LOGGER.debug("matching dCache unit found: {}", unit);
            list.add(unit);
        }
    }

    private void addNetUnit(List<Unit> list, String netUnitName) {
        if (netUnitName != null) {
            try {
                Unit unit;
                if (DEFAULT_IPV4_NET_UNIT.equals(netUnitName)
                      || DEFAULT_IPV6_NET_UNIT.equals(netUnitName)) {
                    unit = _units.get(netUnitName);
                } else {
                    unit = _netHandler.match(netUnitName);
                }

                if (unit == null) {
                    LOGGER.debug("no matching net unit found for: {}", netUnitName);
                    /* for backward compatibility, do not throw exception */
                    return;
                }

                LOGGER.debug("matching net unit found: {}", unit);
                list.add(unit);
            } catch (UnknownHostException uhe) {
                throw new IllegalArgumentException(
                      "NetUnit not resolved : " + netUnitName);
            }
        }
    }

    private LinkGroup resolveLinkGroup(String linkGroupName) {
        LinkGroup linkGroup = null;
        if (linkGroupName != null) {
            linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                LOGGER.debug("LinkGroup not found : {}", linkGroupName);
                throw new IllegalArgumentException("LinkGroup not found : "
                      + linkGroupName);
            }
        }
        return linkGroup;
    }

    private Set<Link> findMatchingLinks(List<Unit> units, LinkGroup linkGroup,
          DirectionType type) {
        Set<Link> sortedSet = new TreeSet<>(new LinkComparator(type));
        LinkMap matchingLinks = new LinkMap();
        int fitCount = units.size();
        for (Unit unit : units) {
            matchingLinks = match(matchingLinks, unit, linkGroup, type);
        }

        Iterator<Link> linkIterator = matchingLinks.iterator();
        while (linkIterator.hasNext()) {
            Link link = linkIterator.next();
            if (link._uGroupList.size() <= fitCount) {
                sortedSet.add(link);
            }
        }

        return sortedSet;
    }

    private List<List<Link>> matchPreferences(DirectionType type, Set<Link> sortedSet) {
        int pref = -1;
        List<List<Link>> linkLists = new ArrayList<>();
        List<Link> currentList = null;

        switch (type) {
            case READ:
                for (Link link : sortedSet) {
                    if (link.getReadPref() < 1) {
                        continue;
                    }
                    if (link.getReadPref() != pref) {
                        linkLists.add(currentList = new ArrayList<>());
                        pref = link.getReadPref();
                    }
                    currentList.add(link);
                }
                break;
            case CACHE:
                for (Link link : sortedSet) {
                    if (link.getCachePref() < 1) {
                        continue;
                    }
                    if (link.getCachePref() != pref) {
                        linkLists.add(currentList = new ArrayList<>());
                        pref = link.getCachePref();
                    }
                    currentList.add(link);
                }
                break;
            case P2P:
                for (Link link : sortedSet) {
                    int tmpPref = link.getP2pPref() < 0 ? link.getReadPref()
                          : link.getP2pPref();
                    if (tmpPref < 1) {
                        continue;
                    }
                    if (tmpPref != pref) {
                        linkLists.add(currentList = new ArrayList<>());
                        pref = tmpPref;
                    }
                    currentList.add(link);
                }
                break;
            case WRITE:
                for (Link link : sortedSet) {
                    if (link.getWritePref() < 1) {
                        continue;
                    }
                    if (link.getWritePref() != pref) {
                        linkLists.add(currentList = new ArrayList<>());
                        pref = link.getWritePref();
                    }
                    currentList.add(link);
                }
        }

        return linkLists;
    }

    private PoolPreferenceLevel[] buildPreferenceLevels(DirectionType type,
          List<List<Link>> linkLists, FileAttributes fileAttributes, Predicate<String> exclude) {
        List<Link>[] linkListsArray = linkLists.toArray(List[]::new);
        PoolPreferenceLevel[] result = new PoolPreferenceLevel[linkListsArray.length];

        for (int i = 0; i < linkListsArray.length; i++) {
            List<Link> linkList = linkListsArray[i];
            List<String> resultList = new ArrayList<>();
            String tag = null;

            for (Link link : linkList) {
                if ((tag == null) && (link.getTag() != null)) {
                    tag = link.getTag();
                }

                for (PoolCore poolCore : link._poolList.values()) {
                    if (poolCore instanceof Pool) {
                        Pool pool = (Pool) poolCore;
                        LOGGER.debug("Pool: {} can read from tape? : {}", pool,
                              pool.canReadFromTape());
                        if (((type == DirectionType.READ && pool.canRead())
                              || (type == DirectionType.CACHE && pool.canReadFromTape()
                              && poolCanStageFile(pool, fileAttributes))
                              || (type == DirectionType.WRITE && pool.canWrite())
                              || (type == DirectionType.P2P && pool.canWriteForP2P()))
                              && (_allPoolsActive || pool.isActive())) {
                            if (exclude.test(pool.getName())) {
                                LOGGER.debug(
                                      "Qualifying pool {} is on excluded host {}; skipping.",
                                      pool.getName(),
                                      pool.getCanonicalHostName());
                            } else {
                                resultList.add(pool.getName());
                            }
                        }
                    } else {
                        for (Pool pool : ((PGroup) poolCore)._poolList.values()) {
                            LOGGER.debug("Pool: {} can read from tape? : {}", pool,
                                  pool.canReadFromTape());
                            if (((type == DirectionType.READ && pool.canRead())
                                  || (type == DirectionType.CACHE && pool.canReadFromTape()
                                  && poolCanStageFile(pool, fileAttributes))
                                  || (type == DirectionType.WRITE && pool.canWrite())
                                  || (type == DirectionType.P2P && pool.canWriteForP2P()))
                                  && (_allPoolsActive || pool.isActive())) {
                                if (exclude.test(pool.getName())) {
                                    LOGGER.debug(
                                          "Qualifying pool {} is on excluded host {}; skipping.",
                                          pool.getName(),
                                          pool.getCanonicalHostName());
                                } else {
                                    resultList.add(pool.getName());
                                }
                            }
                        }
                    }
                }
            }
            result[i] = new PoolPreferenceLevel(resultList, tag);
        }

        return result;
    }

    private void logResult(PoolPreferenceLevel[] result) {
        StringBuilder sb = new StringBuilder("match done: ");

        for (int i = 0; i < result.length; i++) {
            sb.append("[").append(i).append("] :");
            for (String poolName : result[i].getPoolList()) {
                sb.append(" ").append(poolName);
            }
        }

        LOGGER.debug(sb.toString());
    }

    private String printPreferenceLevels(PoolPreferenceLevel[] list) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.length; i++) {
            String tag = list[i].getTag();
            sb.append("Preference : ").append(i).append("\n");
            sb.append("       Tag : ").append(tag == null ? "NONE" : tag)
                  .append("\n");
            for (String s : list[i].getPoolList()) {
                sb.append("  ").append(s)
                      .append("\n");
            }
        }

        return sb.toString();
    }

    @Override
    public String getProtocolUnit(String protocolUnitName) {
        Unit unit = findProtocolUnit(protocolUnitName);
        return unit == null ? null : unit.getName();
    }

    //
    // Legal formats : <protocol>/<version>
    //

    public Unit findProtocolUnit(String protocolUnitName) {
        //
        if ((protocolUnitName == null) || (protocolUnitName.isEmpty())) {
            return null;
        }
        //
        int position = protocolUnitName.indexOf('/');
        //
        //
        if ((position < 0) || (position == 0)
              || (position == (protocolUnitName.length() - 1))) {

            throw new IllegalArgumentException(
                  "Not a valid protocol specification : " + protocolUnitName);
        }
        //
        // we try :
        // <protocol>/<majorVersion>
        // <protocol>/*
        // */*
        //
        Unit unit = null;
        rlock();
        try {
            unit = _units.get(protocolUnitName);
            if (unit != null) {
                return unit;
            }

            //
            //
            unit = _units.get(protocolUnitName.substring(0, position) + "/*");
            //
            if (unit == null) {
                unit = _units.get("*/*");
            }

        } finally {
            runlock();
        }
        //
        return unit;

    }

    @Override
    public StorageUnit getStorageUnit(String storageClass) {
        _psuReadLock.lock();
        try {
            Unit unit = _units.get(storageClass);
            if (unit != null && unit.getType() == STORE) {
                return (StorageUnit) unit;
            }
        } finally {
            _psuReadLock.unlock();
        }

        return null;
    }

    @Override
    public String getNetIdentifier(String address) throws UnknownHostException {

        rlock();
        try {
            NetUnit unit = _netHandler.match(address);
            if (unit == null) {
                return NO_NET;
            }
            return unit._uGroupList.isEmpty() ? unit.getName() :
                  unit._uGroupList.keySet().stream().collect(Collectors.joining("-"));
        } finally {
            runlock();
        }
    }

    /**
     * Picks links associated with a unit (elementary rule).
     *
     * @param unit      The unit as the matching criteria
     * @param linkGroup Use only subset of links if defined, or all associated links if not defined
     *                  (null)
     * @return the matching links
     */
    public Map<String, Link> match(Unit unit, LinkGroup linkGroup, DirectionType iotype) {

        Map<String, Link> map = new HashMap<>();

        rlock();
        try {
            for (UGroup uGroup : unit._uGroupList.values()) {
                for (Link link : uGroup._linkList.values()) {

                    if (linkGroup == null) {
                        if (iotype == DirectionType.READ
                              || link.getLinkGroup() == null) {
                            //
                            // no link group specified
                            // only consider link if it isn't in any link group
                            // ( "default link group" )
                            //
                            LOGGER.debug("link {} matching to unit {}", link.getName(), unit);
                            map.put(link.getName(), link);
                        }
                    } else if (linkGroup.contains(link)) {
                        //
                        // only take link if it is in the specified link group
                        //
                        LOGGER.debug("link {} matching to unit {}", link.getName(), unit);
                        map.put(link.getName(), link);
                    }
                }
            }
        } finally {
            runlock();
        }
        return map;
    }

    public void setAllPoolsActive(String mode) {
        wlock();
        try {
            switch (mode) {
                case "on":
                case "true":
                    _allPoolsActive = true;
                    break;
                case "off":
                case "false":
                    _allPoolsActive = false;
                    break;
                default:
                    throw new IllegalArgumentException(
                          "Syntax error," + " no such mode: " + mode);
            }
        } finally {
            wunlock();
        }
    }

    public String netMatch(String hostAddress) throws UnknownHostException {
        NetUnit unit = null;

        rlock();
        try {
            unit = _netHandler.match(hostAddress);
        } finally {
            runlock();
        }
        if (unit == null) {
            throw new IllegalArgumentException("Host not a unit : "
                  + hostAddress);
        }
        return unit.toString();
    }

    public String matchLinkGroups(String linkGroup, String direction, String storeUnit,
          String dCacheUnit, String netUnit, String protocolUnit) {
        PoolPreferenceLevel[] list = matchLinkGroupsXml(linkGroup, direction, storeUnit, dCacheUnit,
              netUnit, protocolUnit);
        return printPreferenceLevels(list);
    }

    public String matchUnits(String netUnitName, ImmutableList<String> units) {
        StringBuilder sb = new StringBuilder();
        Map<String, Link> map = null;
        int required = units.size();

        rlock();
        try {
            for (String unitName : units) {
                Unit unit = _units.get(unitName);
                if (unit == null) {
                    throw new IllegalArgumentException("Unit not found : "
                          + unitName);
                }
                map = match(map, unit, DirectionType.READ);
            }
            if (netUnitName != null) {
                Unit unit = _netHandler.find(new NetUnit(netUnitName));
                if (unit == null) {
                    throw new IllegalArgumentException(
                          "Unit not found in netList : " + netUnitName);
                }
                map = match(map, unit, DirectionType.READ);
            }
            for (Link link : map.values()) {
                if (link._uGroupList.size() != required) {
                    continue;
                }
                sb.append("Link : ").append(link.toString()).append("\n");

                for (SelectionPool pool : link.getPools()) {
                    sb.append("    ").append(pool.getName()).append(
                          "\n");
                }
            }

        } finally {
            runlock();
        }
        return sb.toString();
    }

    // /////////////////////////////////////////////////////////////////////////////
    //
    // the CLI
    //
    // ..............................................................
    //
    // the create's
    //

    public void createDynamicPoolGroup(String name, boolean isPrimary, Map<String, String> tags) {
        wlock();
        try {
            DynamicPGroup group = new DynamicPGroup(name, isPrimary, tags);

            if (_pGroups.putIfAbsent(group.getName(), group) != null) {
                throw new IllegalArgumentException("Duplicated entry : " + name);
            }
            // add any existing pools
            _pools.values().forEach(p -> group.addIfMatches(p));
        } finally {
            wunlock();
        }
    }

    public void createPoolGroup(String name, boolean isPrimary) {
        wlock();
        try {
            PGroup group = new PGroup(name, isPrimary);

            if (_pGroups.putIfAbsent(group.getName(), group) != null) {
                throw new IllegalArgumentException("Duplicated entry : " + name);
            }
        } finally {
            wunlock();
        }
    }

    @Override
    public boolean isEnabledRegex() {
        return _useRegex;
    }

    public String setRegex(String onOff) {
        String retVal;
        switch (onOff) {
            case "on":
                _useRegex = true;
                retVal = "regex turned on";
                break;
            case "off":
                _useRegex = false;
                retVal = "regex turned off";
                break;
            default:
                throw new IllegalArgumentException(
                      "please set regex either on or off");
        }
        return retVal;
    }

    public void createPool(String name, boolean isNoPing, boolean isDisabled, boolean isReadOnly) {
        wlock();
        try {
            if (_pools.get(name) != null) {
                throw new IllegalArgumentException("Duplicated entry : " + name);
            }

            Pool pool = new Pool(name);
            pool.setPing(!isNoPing);
            pool.setEnabled(!isDisabled);
            pool.setReadOnly(isReadOnly);

            _pools.put(pool.getName(), pool);
        } finally {
            wunlock();
        }
    }

    public String setPool(String glob, String mode) {
        Pattern pattern = Glob.parseGlobToPattern(glob);
        wlock();
        try {
            int count = 0;
            for (Pool pool : getPools(pattern)) {
                count++;
                switch (mode) {
                    case "enabled":
                        pool.setEnabled(true);
                        break;
                    case "disabled":
                        pool.setEnabled(false);
                        break;
                    case "ping":
                        pool.setPing(true);
                        break;
                    case "noping":
                        pool.setPing(false);
                        break;
                    case "rdonly":
                        pool.setReadOnly(true);
                        break;
                    case "notrdonly":
                        pool.setReadOnly(false);
                        break;
                    default:
                        throw new IllegalArgumentException("mode not supported : "
                              + mode);
                }
            }
            return poolCountDescriptionFor(count) + " updated";
        } finally {
            wunlock();
        }
    }

    public String setPoolEnabled(String poolName) {
        int count = setEnabled(new Glob(poolName), true);
        return poolCountDescriptionFor(count) + " enabled";
    }

    public String setPoolDisabled(String poolName) {
        int count = setEnabled(new Glob(poolName), false);
        return poolCountDescriptionFor(count) + " disabled";
    }

    public void createLink(String name, ImmutableList<String> unitGroup) {
        wlock();
        try {

            if (_links.get(name) != null) {
                throw new IllegalArgumentException("Duplicated entry : " + name);
            }

            Link link = new Link(name);
            //
            // we have to check if all the ugroups really exists.
            // only after we know, that all exist we can
            // add ourselfs to the uGroupLinkList
            //
            for (String uGroupName : unitGroup) {
                UGroup uGroup = _uGroups.get(uGroupName);
                if (uGroup == null) {
                    throw new IllegalArgumentException("uGroup not found : "
                          + uGroupName);
                }

                link._uGroupList.put(uGroup.getName(), uGroup);

            }
            for (UGroup group : link._uGroupList.values()) {
                group._linkList.put(link.getName(), link);
            }
            _links.put(link.getName(), link);

        } finally {
            wunlock();
        }
    }

    public void createUnitGroup(String name) {
        wlock();
        try {
            if (_uGroups.get(name) != null) {
                throw new IllegalArgumentException("Duplicated entry : " + name);
            }

            UGroup group = new UGroup(name);

            _uGroups.put(group.getName(), group);
        } finally {
            wunlock();
        }
    }

    public void createUnit(String name, boolean isNet, boolean isStore,
          boolean isDcache, boolean isProtocol) {
        Unit unit = null;
        wlock();
        try {
            if (isNet) {
                NetUnit net = new NetUnit(name);
                _netHandler.add(net);
                unit = net;
            } else if (isStore) {
                unit = new StorageUnit(name);
            } else if (isDcache) {
                unit = new Unit(name, DCACHE);
            } else if (isProtocol) {
                unit = new ProtocolUnit(name);
            }
            if (unit == null) {
                throw new IllegalArgumentException(
                      "Unit type missing net/store/dcache/protocol");
            }

            if (_units.putIfAbsent(name, unit) != null) {
                throw new IllegalArgumentException("Duplicated entry : " + name);
            }
        } finally {
            wunlock();
        }
    }

    public void createLinkGroup(String groupName, boolean isReset) {
        wlock();
        try {

            if (_linkGroups.containsKey(groupName) && !isReset) {
                throw new IllegalArgumentException(
                      "LinkGroup already exists : " + groupName);
            }

            LinkGroup newGroup = new LinkGroup(groupName);
            _linkGroups.put(groupName, newGroup);
        } finally {
            wunlock();
        }
    }

    public void setStorageUnit(String storageUnitKey,
          Integer required,
          String[] onlyOneCopyPer) {
        wlock();
        try {
            Unit unit = _units.get(storageUnitKey);

            if (unit == null) {
                throw new IllegalArgumentException("Not found : " + storageUnitKey);
            }

            if (unit.getType() != STORE) {
                throw new IllegalStateException("unit named "
                      + storageUnitKey + " is not of type STORE");
            }

            StorageUnit sUnit = (StorageUnit) unit;
            sUnit.setRequiredCopies(required);

            if (required == null) {
                sUnit.setOnlyOneCopyPer(null);
            }

            if (onlyOneCopyPer != null) {
                Preconditions.checkArgument(sUnit.getRequiredCopies() != null,
                      "required must be set to >= 1 "
                            + "in order to set partition tags, "
                            + "is currently set to none.");
            }

            sUnit.setOnlyOneCopyPer(onlyOneCopyPer);
        } finally {
            wunlock();
        }
    }

    //
    // ..................................................................
    //
    // the 'psux ... ls'
    //
    public Object listPoolXml(String poolName) {
        Object xlsResult = null;
        rlock();
        try {
            if (poolName == null) {
                xlsResult = _pools.keySet().toArray();
            } else {
                Pool pool = _pools.get(poolName);
                if (pool == null) {
                    throw new IllegalArgumentException("Not found : "
                          + poolName);
                }

                Object[] result = new Object[6];
                result[0] = poolName;
                result[1] = pool._pGroupList.keySet().toArray();
                result[2] = pool._linkList.keySet().toArray();
                result[3] = pool.isEnabled();
                result[4] = pool.getActive();
                result[5] = pool.isReadOnly();
                xlsResult = result;
            }
        } finally {
            runlock();
        }

        return xlsResult;
    }

    public Object listPoolGroupXml(String groupName) {
        Object xlsResult = null;
        rlock();
        try {

            if (groupName == null) {
                xlsResult = _pGroups.keySet().toArray();
            } else {
                PGroup group = _pGroups.get(groupName);
                if (group == null) {
                    throw new IllegalArgumentException("Not found : "
                          + groupName);
                }

                Object[] result = new Object[4];
                result[0] = groupName;
                result[1] = group._poolList.keySet().toArray();
                result[2] = group._linkList.keySet().toArray();
                result[3] = group.isPrimary();
                xlsResult = result;
            }
        } finally {
            runlock();
        }

        return xlsResult;
    }

    @Override
    public Map<String, SelectionPoolGroup> getPoolGroups() {
        rlock();
        try {
            return Maps.newHashMap(_pGroups);
        } finally {
            runlock();
        }
    }

    public Object listUnitXml(String unitName) {
        Object xlsResult = null;
        rlock();
        try {

            if (unitName == null) {
                xlsResult = _units.keySet().toArray();
            } else {
                Unit unit = _units.get(unitName);
                if (unit == null) {
                    throw new IllegalArgumentException("Not found : "
                          + unitName);
                }

                Object[] result = new Object[5];
                result[0] = unitName;
                result[1] = unit.getType() == STORE ? "Store"
                      : unit.getType() == PROTOCOL ? "Protocol"
                            : unit.getType() == DCACHE ? "dCache"
                                  : unit.getType() == NET ? "Net" : "Unknown";
                result[2] = unit._uGroupList.keySet().toArray();
                if ("Store".equals(result[1])) {
                    StorageUnit sunit = (StorageUnit) unit;
                    result[3] = sunit.getRequiredCopies();
                    result[4] = sunit.getOnlyOneCopyPer();
                }
                xlsResult = result;
            }
        } finally {
            runlock();
        }

        return xlsResult;
    }

    public Object listUnitGroupXml(String groupName) {
        Object xlsResult = null;
        rlock();
        try {
            if (groupName == null) {
                xlsResult = _uGroups.keySet().toArray();
            } else {
                UGroup group = _uGroups.get(groupName);
                if (group == null) {
                    throw new IllegalArgumentException("Not found : "
                          + groupName);
                }

                Object[] result = new Object[3];
                result[0] = groupName;
                result[1] = group._unitList.keySet().toArray();
                result[2] = group._linkList.keySet().toArray();
                xlsResult = result;
            }
        } finally {
            runlock();
        }

        return xlsResult;
    }

    public Object listLinkXml(boolean isX, boolean resolve, String linkName) {
        Object xlsResult = null;
        rlock();
        try {

            if (linkName == null) {
                if (!isX) {
                    xlsResult = _links.keySet().toArray();
                } else {
                    List<Object[]> array = new ArrayList<>();
                    for (Link link : _links.values()) {
                        array.add(fillLinkProperties(link, resolve));
                    }
                    xlsResult = array;
                }
            } else {
                Link link = _links.get(linkName);
                if (link == null) {
                    throw new IllegalArgumentException("Not found : "
                          + linkName);
                }

                xlsResult = fillLinkProperties(link, resolve);
            }
        } finally {
            runlock();
        }

        return xlsResult;
    }

    private Object[] fillLinkProperties(Link link, boolean resolve) {
        List<String> pools = new ArrayList<>();
        List<String> groups = new ArrayList<>();
        for (PoolCore core : link._poolList.values()) {
            if (core instanceof Pool) {
                pools.add(core.getName());
            } else {
                groups.add(core.getName());
                if (!resolve) {
                    continue;
                }
                PGroup pg = (PGroup) core;
                if (pg._poolList == null) {
                    continue;
                }
                for (String poolName : pg._poolList.keySet()) {
                    pools.add(poolName);
                }
            }
        }

        Object[] result = new Object[resolve ? 13 : 9];
        result[0] = link.getName();
        result[1] = link.getReadPref();
        result[2] = link.getCachePref();
        result[3] = link.getWritePref();
        result[4] = link._uGroupList.keySet().toArray();
        result[5] = pools.toArray();
        result[6] = groups.toArray();
        result[7] = link.getP2pPref();
        result[8] = link.getTag();

        if ((!resolve) || (link._uGroupList == null)) {
            return result;
        }

        List<String> net = new ArrayList<>();
        List<String> protocol = new ArrayList<>();
        List<String> dcache = new ArrayList<>();
        List<String> store = new ArrayList<>();

        for (UGroup ug : link._uGroupList.values()) {
            if (ug._unitList == null) {
                continue;
            }
            for (Unit unit : ug._unitList.values()) {
                switch (unit.getType()) {
                    case NET:
                        net.add(unit.getName());
                        break;
                    case PROTOCOL:
                        protocol.add(unit.getName());
                        break;
                    case DCACHE:
                        dcache.add(unit.getName());
                        break;
                    case STORE:
                        store.add(unit.getName());
                        break;
                }
            }
            result[9] = store.toArray();
            result[10] = net.toArray();
            result[11] = dcache.toArray();
            result[12] = protocol.toArray();
        }

        return result;
    }

    public PoolPreferenceLevel[] matchLinkGroupsXml(String linkGroup,
          String direction,
          String storeUnit,
          String dCacheUnit,
          String netUnit,
          String protocolUnit) {
        StorageInfo info = GenericStorageInfo.valueOf(storeUnit, dCacheUnit);
        FileAttributes fileAttributes = FileAttributes.ofStorageInfo(info);
        return match(DirectionType.valueOf(direction.toUpperCase()),
              netUnit.equals("*") ? DEFAULT_IPV6_NET_UNIT : netUnit,
              protocolUnit.equals("*") ? DEFAULT_PROTOCOL_UNIT : protocolUnit,
              fileAttributes, linkGroup, p -> false);
    }

    // ..................................................................
    //
    // the 'ls'
    //
    public String listPool(boolean more, boolean detail, ImmutableList<String> globs) {
        StringBuilder sb = new StringBuilder();
        rlock();
        try {
            Stream<Pool> pools;
            if (globs.isEmpty()) {
                pools = _pools.values().stream();
            } else {
                pools = globs.stream().flatMap(s -> getPools(Glob.parseGlobToPattern(s)).stream());
            }
            pools.sorted(comparing(Pool::getName)).forEachOrdered(
                  pool -> {
                      if (!detail) {
                          sb.append(pool.getName()).append("\n");
                      } else {
                          sb.append(pool).append("\n");
                          sb.append(" linkList   :\n");
                          pool._linkList.values().stream().sorted(comparing(Link::getName))
                                .forEachOrdered(
                                      link -> sb.append("   ").append(link).append("\n"));
                          if (more) {
                              sb.append(" pGroupList : \n");
                              pool._pGroupList.values().stream().sorted(comparing(PGroup::getName))
                                    .forEachOrdered(
                                          group -> sb.append("   ").append(group).append("\n"));
                          }
                      }
                  });
        } finally {
            runlock();
        }
        return sb.toString();
    }

    public String listPoolGroups(boolean more, boolean detail, ImmutableList<String> groups) {
        StringBuilder sb = new StringBuilder();
        rlock();
        try {
            Iterator<PGroup> i;
            if (groups.isEmpty()) {
                i = _pGroups.values().iterator();
            } else {
                ArrayList<PGroup> l = new ArrayList<>();
                groups.stream().forEach((group) -> {
                    PGroup o = _pGroups.get(group);
                    if (o != null) {
                        l.add(o);
                    }
                });
                i = l.iterator();
            }
            while (i.hasNext()) {
                PGroup group = i.next();
                sb.append(group.getName()).append("\n");
                if (detail) {
                    sb.append(" dynamic   = ").append(group instanceof DynamicPGroup).append("\n")
                          .append(" resilient = ").append(group.isPrimary())
                          .append("\n")
                          .append(" linkList :\n");
                    group._linkList.values().stream().sorted(comparing(Link::getName))
                          .forEachOrdered(
                                link -> sb.append("   ").append(link.toString()).append("\n"));
                    sb.append(" poolList :\n");
                    group._poolList.values().stream().sorted(comparing(Pool::getName))
                          .forEachOrdered(
                                pool -> sb.append("   ").append(pool.toString()).append("\n"));
                    sb.append("nested groups  = ").append(group._pgroupList).append("\n") ;
                }
            }
        } finally {
            runlock();
        }
        return sb.toString();
    }

    public String listPoolLinks(boolean more, boolean detail, ImmutableList<String> names) {
        StringBuilder sb = new StringBuilder();
        rlock();
        try {
            Stream<Link> links;
            if (names.isEmpty()) {
                links = _links.values().stream();
            } else {
                links = names.stream().map(_links::get).filter(Objects::nonNull);
            }
            links.sorted(comparing(Link::getName)).forEachOrdered(
                  link -> {
                      sb.append(link.getName()).append("\n");
                      if (detail) {
                          sb.append(" readPref  : ").append(link.getReadPref()).append(
                                "\n");
                          sb.append(" cachePref : ").append(link.getCachePref()).append(
                                "\n");
                          sb.append(" writePref : ").append(link.getWritePref()).append(
                                "\n");
                          sb.append(" p2pPref   : ").append(link.getP2pPref()).append(
                                "\n");
                          sb.append(" section   : ").append(
                                      link.getTag() == null ? "None" : link.getTag())
                                .append("\n");
                          sb.append(" linkGroup : ").append(
                                link.getLinkGroup() == null ? "None" : link
                                      .getLinkGroup().getName()).append("\n");
                          sb.append(" UGroups :\n");
                          link._uGroupList.values().stream().sorted(comparing(UGroup::getName))
                                .forEachOrdered(
                                      group -> sb.append("   ").append(group.toString())
                                            .append("\n"));
                          if (more) {
                              sb.append(" poolList  :\n");
                              link._poolList.values().stream().sorted(comparing(PoolCore::getName))
                                    .forEachOrdered(
                                          core -> sb.append("   ").append(core.toString())
                                                .append("\n"));
                          }
                      }
                  });
        } finally {
            runlock();
        }
        return sb.toString();
    }

    public String listUnitGroups(boolean more, boolean detail, ImmutableList<String> names) {

        StringBuilder sb = new StringBuilder();
        rlock();
        try {
            Stream<UGroup> i;
            if (names.isEmpty()) {
                i = _uGroups.values().stream();
            } else {
                i = names.stream().map(_uGroups::get).filter(Objects::nonNull);
            }
            i.sorted(comparing(UGroup::getName)).forEachOrdered(
                  group -> {
                      sb.append(group.getName()).append("\n");
                      if (detail) {
                          sb.append(" unitList :\n");
                          group._unitList.values().stream().sorted(comparing(Unit::getName))
                                .forEachOrdered(
                                      unit -> sb.append("   ").append(unit.toString())
                                            .append("\n"));
                          if (more) {
                              sb.append(" linkList :\n");
                              group._linkList.values().stream().sorted(comparing(Link::getName))
                                    .forEachOrdered(
                                          link -> sb.append("   ").append(link.toString())
                                                .append("\n"));
                          }
                      }
                  });
        } finally {
            runlock();
        }
        return sb.toString();
    }

    public String listNetUnits() {
        StringBuilder sb = new StringBuilder();

        rlock();
        try {
            Streams.concat(_netHandler._netList.stream(), _netHandler._netListV6.stream())
                  .forEach(u -> {
                      sb.append(u.getHostAddress().getHostAddress());
                      int mask = u.getMask();
                      if (mask > 0) {
                          sb.append('/').append(mask);
                      }
                      sb.append('\n');
                  });
        } finally {
            runlock();
        }
        return sb.toString();
    }

    public String listUnits(boolean more, boolean detail, ImmutableList<String> names) {
        StringBuilder sb = new StringBuilder();
        rlock();
        try {
            Stream<Unit> i;
            if (names.isEmpty()) {
                i = _units.values().stream();
            } else {
                i = names.stream().map(_units::get).filter(Objects::nonNull);
            }
            i.sorted(comparing(Unit::getName)).forEachOrdered(
                  unit -> {
                      if (detail) {
                          sb.append(unit.toString()).append("\n");
                          if (more) {
                              sb.append(" uGroupList :\n");
                              unit._uGroupList.values().stream().sorted(
                                    comparing(UGroup::getName)).forEachOrdered(
                                    group -> sb.append(
                                          "   ").append(
                                          group.toString()).append(
                                          "\n"));
                          }
                      } else {
                          sb.append(unit.getName()).append("\n");
                      }
                  });
        } finally {
            runlock();
        }
        return sb.toString();
    }

    public String listLinkGroups(boolean isLongOutput, ImmutableList<String> linkGroups) {
        StringBuilder sb = new StringBuilder();

        rlock();
        try {
            if (!linkGroups.isEmpty()) {
                linkGroups.stream().forEachOrdered(lgroup -> {
                    LinkGroup linkGroup = _linkGroups.get(lgroup);
                    if (linkGroup == null) {
                        throw new IllegalArgumentException(
                              "LinkGroup not found : " + lgroup);
                    }

                    if (isLongOutput) {
                        sb.append(linkGroup).append("\n");
                    } else {
                        sb.append(lgroup).append("\n");
                    }
                });
            } else {
                _linkGroups.values().stream().sorted(comparing(LinkGroup::getName)).forEachOrdered(
                      linkGroup -> sb.append(isLongOutput ? linkGroup : linkGroup.getName())
                            .append("\n"));
            }
        } finally {
            runlock();
        }

        return sb.toString();
    }

    public String dumpSetup() {
        StringWriter s = new StringWriter();
        printSetup(new PrintWriter(s));
        return s.toString();
    }

    //
    // .............................................................................
    //
    // the 'removes'
    //
    public void removeUnit(String name, boolean isNet) {
        wlock();
        try {
            if (isNet) {
                NetUnit netUnit = _netHandler.find(new NetUnit(name));
                if (netUnit == null) {
                    throw new IllegalArgumentException(
                          "Not found in netList : " + name);
                }
                name = netUnit.getName();
            }
            Unit unit = _units.get(name);
            if (unit == null) {
                throw new IllegalArgumentException("Unit not found : "
                      + name);
            }

            if (unit instanceof NetUnit) {
                _netHandler.remove((NetUnit) unit);
            }

            for (UGroup group : unit._uGroupList.values()) {
                group._unitList.remove(unit.getCanonicalName());
            }

            _units.remove(name);
        } finally {
            wunlock();
        }
    }

    public void removeUnitGroup(String name) {
        wlock();
        try {
            UGroup group = _uGroups.get(name);
            if (group == null) {
                throw new IllegalArgumentException("UGroup not found : "
                      + name);
            }

            if (!group._unitList.isEmpty()) {
                throw new IllegalArgumentException("UGroup not empty : "
                      + name);
            }

            if (!group._linkList.isEmpty()) {
                throw new IllegalArgumentException(
                      "Still link(s) pointing to us : " + name);
            }

            _uGroups.remove(name);

        } finally {
            wunlock();
        }
    }

    public void removePoolGroup(String name) {
        wlock();
        try {
            PGroup group = _pGroups.get(name);
            if (group == null) {
                throw new IllegalArgumentException("PGroup not found : " + name);
            }

            if (group instanceof DynamicPGroup) {
                // we can always remove dynamic pool groups
                group._poolList.values().forEach(p -> p._pGroupList.remove(name));
                group._poolList.clear();
            }

            //
            // check if empty
            //
            if (!group._poolList.isEmpty()) {
                throw new IllegalArgumentException("PGroup not empty : " + name);
            }
            //
            // remove the links
            //
            for (Link link : group._linkList.values()) {
                link._poolList.remove(group.getName());
            }
            //
            // remove from global
            //
            _pGroups.remove(name);
        } finally {
            wunlock();
        }
    }

    public void removePool(String name) {
        wlock();
        try {
            Pool pool = _pools.get(name);
            if (pool == null) {
                throw new IllegalArgumentException("Pool not found : " + name);
            }
            //
            // remove from groups
            //
            for (PGroup group : pool._pGroupList.values()) {
                group._poolList.remove(pool.getName());
            }
            //
            // remove the links
            //
            for (Link link : pool._linkList.values()) {
                link._poolList.remove(pool.getName());
            }
            //
            // remove from global
            //
            _pools.remove(name);
        } finally {
            wunlock();
        }
    }

    public void removeFromUnitGroup(String groupName, String unitName, boolean isNet) {
        wlock();
        try {
            UGroup group = _uGroups.get(groupName);
            if (group == null) {
                throw new IllegalArgumentException("UGroup not found : "
                      + groupName);
            }

            if (isNet) {
                NetUnit netUnit = _netHandler.find(new NetUnit(unitName));
                if (netUnit == null) {
                    throw new IllegalArgumentException(
                          "Not found in netList : " + unitName);
                }
                unitName = netUnit.getName();
            }
            Unit unit = _units.get(unitName);
            if (unit == null) {
                throw new IllegalArgumentException("Unit not found : "
                      + unitName);
            }
            String canonicalName = unit.getCanonicalName();
            if (group._unitList.get(canonicalName) == null) {
                throw new IllegalArgumentException(unitName + " not member of "
                      + groupName);
            }

            group._unitList.remove(canonicalName);
            unit._uGroupList.remove(groupName);
        } finally {
            wunlock();
        }
    }

    public void removeFromPoolGroup(String groupName, String poolName) {
        wlock();
        try {

            PGroup group = _pGroups.get(groupName);
            if (group == null) {
                throw new IllegalArgumentException("PGroup not found : "
                      + groupName);
            }

            if (poolName.charAt(0) == '@') {
                PGroup nestedGroup = _pGroups.get(poolName.substring(1));
                if (nestedGroup == null) {
                    throw new IllegalArgumentException("Group not found : " + poolName);
                }

                if (!group._pgroupList.remove(nestedGroup)) {
                    throw new IllegalArgumentException(nestedGroup + " not a subgroup of "
                          + groupName);
                }
            } else {

                Pool pool = _pools.get(poolName);
                if (pool == null) {
                    throw new IllegalArgumentException("Pool not found : "
                          + poolName);
                }

                if (group._poolList.get(poolName) == null) {
                    throw new IllegalArgumentException(poolName + " not member of "
                          + groupName);
                }

                group._poolList.remove(poolName);
                pool._pGroupList.remove(groupName);
            }
        } finally {
            wunlock();
        }
    }

    public void removeFromLinkGroup(String linkGroupName, String linkName) {
        wlock();
        try {

            LinkGroup linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                      + linkGroupName);
            }

            Link link = _links.get(linkName);
            if (link == null) {
                throw new IllegalArgumentException("Link is not found : "
                      + linkName);
            }

            if (!linkGroup.remove(link)) {
                throw new IllegalArgumentException("Link [" + linkName
                      + "] is not part of group : " + linkGroupName);
            }
            link.setLinkGroup(null);

        } finally {
            wunlock();
        }
    }

    public void removeLinkGroup(String name) {
        wlock();
        try {

            LinkGroup linkGroup = _linkGroups.remove(name);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                      + name);
            }

            for (SelectionLink link : linkGroup.getAllLinks()) {
                if (link instanceof Link) {
                    ((Link) link).setLinkGroup(null);
                }
            }

        } finally {
            wunlock();
        }
    }

    public void removeLink(String name) {
        wlock();
        try {
            Link link = _links.get(name);
            if (link == null) {
                throw new IllegalArgumentException("Link not found : " + name);
            }
            //
            // remove from pools
            //
            for (PoolCore core : link._poolList.values()) {
                core._linkList.remove(name);
            }
            //
            // remove from unit group
            //
            for (UGroup group : link._uGroupList.values()) {
                group._linkList.remove(name);
            }
            //
            // remove from link group. A link can be in exactly one linkgroup at
            // the same time.
            //
            for (LinkGroup lGroup : _linkGroups.values()) {
                if (lGroup.remove(link)) {
                    break;
                }
            }
            //
            // remove from global
            //
            _links.remove(name);
        } finally {
            wunlock();
        }
    }

    //
    // ........................................................................
    //
    // relations
    //

    public void addToPoolGroup(String pGroupName, String poolName) {
        wlock();
        try {
            PGroup group = _pGroups.get(pGroupName);
            if (group == null) {
                throw new IllegalArgumentException("Not found : " + pGroupName);
            }
            if (group instanceof DynamicPGroup) {
                throw new IllegalArgumentException(
                      "Manual adding into dynamic pool is not allowed");
            }

            if (poolName.charAt(0) == '@') {
                var groupName = poolName.substring(1);
                PGroup subGroup = _pGroups.get(groupName);
                if (subGroup == null) {
                    throw new IllegalArgumentException("Subgroup not found : " + groupName);
                }
                group.addSubgroup(subGroup);
            } else {
                Pool pool = _pools.get(poolName);
                if (pool == null) {
                    throw new IllegalArgumentException("Pool not found: : " + poolName);
                }

                pool._pGroupList.put(group.getName(), group);
                group._poolList.put(pool.getName(), pool);
            }
        } finally {
            wunlock();
        }
    }

    public void addToUnitGroup(String uGroupName, String unitName, boolean isNet) {
        wlock();
        try {
            if (isNet) {
                NetUnit netUnit = _netHandler.find(new NetUnit(unitName));
                if (netUnit == null) {
                    throw new IllegalArgumentException(
                          "Not found in netList : " + unitName);
                }
                unitName = netUnit.getName();
            }
            UGroup group = _uGroups.get(uGroupName);
            if (group == null) {
                throw new IllegalArgumentException("Not found : " + uGroupName);
            }
            Unit unit = _units.get(unitName);
            if (unit == null) {
                throw new IllegalArgumentException("Not found : " + unitName);
            }

            String canonicalName = unit.getCanonicalName();
            if (group._unitList.get(canonicalName) != null) {
                throw new IllegalArgumentException(unitName
                      + " already member of " + uGroupName);
            }

            unit._uGroupList.put(group.getName(), group);
            group._unitList.put(canonicalName, unit);
        } finally {
            wunlock();
        }
    }

    public void addToLinkGroup(String linkGroupName, String linkName) {
        wlock();
        try {
            LinkGroup linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                      + linkName);
            }

            Link link = _links.get(linkName);
            if (link == null) {
                throw new IllegalArgumentException("Link not found : "
                      + linkName);
            }

            // search all linkgroups for this link
            // a link can be only in one link group at the same time
            for (LinkGroup group : _linkGroups.values()) {
                if (group.contains(link)) {
                    throw new IllegalArgumentException(
                          "Link already in linkGroup `" + group.getName()
                                + "`");
                }
            }

            linkGroup.add(link);
            link.setLinkGroup(linkGroup);
        } finally {
            wunlock();
        }
    }

    public void unlink(String linkName, String poolName) {
        wlock();
        try {
            Link link = _links.get(linkName);
            if (link == null) {
                throw new IllegalArgumentException("Not found : " + linkName);
            }

            PoolCore core = _pools.get(poolName);
            if (core == null) {
                core = _pGroups.get(poolName);
            }
            if (core == null) {
                throw new IllegalArgumentException("Not found : " + poolName);
            }

            if (core._linkList.get(linkName) == null) {
                throw new IllegalArgumentException(poolName + " not member of "
                      + linkName);
            }

            core._linkList.remove(linkName);
            link._poolList.remove(poolName);
        } finally {
            wunlock();
        }
    }

    public void addLink(String linkName, String poolName) {
        wlock();
        try {
            Link link = _links.get(linkName);
            if (link == null) {
                throw new IllegalArgumentException("Not found : " + linkName);
            }

            PoolCore core = _pools.get(poolName);
            if (core == null) {
                core = _pGroups.get(poolName);
                if (core == null) {
                    throw new IllegalArgumentException("Not found : " + poolName);
                }
            }

            core._linkList.put(link.getName(), link);
            link._poolList.put(core.getName(), core);
        } finally {
            wunlock();
        }
    }

    public void setPoolActive(String poolName, boolean active) {
        wlock();
        try {
            if (poolName.equals("*")) {
                for (Pool pool : _pools.values()) {
                    pool.setActive(active);
                }
            } else {
                Pool pool = _pools.get(poolName);
                if (pool == null) {
                    throw new IllegalArgumentException("Pool not found : "
                          + poolName);
                }
                pool.setActive(active);
            }

        } finally {
            wunlock();
        }
    }

    public void setLink(String linkName, String readPref, String writePref,
          String cachePref, String p2pPref, String section) {
        wlock();

        try {
            Link link = _links.get(linkName);
            if (link == null) {
                throw new IllegalArgumentException("Not found : " + linkName);
            }

            if (readPref != null) {
                link.setReadPref(Integer.parseInt(readPref));
            }
            if (writePref != null) {
                link.setWritePref(Integer.parseInt(writePref));
            }
            if (cachePref != null) {
                link.setCachePref(Integer.parseInt(cachePref));
            }
            if (p2pPref != null) {
                link.setP2pPref(Integer.parseInt(p2pPref));
            }
            if (section != null) {
                link.setTag(section.equalsIgnoreCase("NONE") ? null : section);
            }
        } finally {
            wunlock();
        }
    }

    public void setLinkGroup(String linkGroupName, String custodial,
          String nearline, String online, String output,
          String replica) {
        wlock();

        try {
            LinkGroup linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                      + linkGroupName);
            }

            if (custodial != null) {
                linkGroup.setCustodialAllowed(Boolean.parseBoolean(custodial));
            }

            if (nearline != null) {
                linkGroup.setNearlineAllowed(Boolean.parseBoolean(nearline));
            }

            if (online != null) {
                linkGroup.setOnlineAllowed(Boolean.parseBoolean(online));
            }

            if (output != null) {
                linkGroup.setOutputAllowed(Boolean.parseBoolean(output));
            }

            if (replica != null) {
                linkGroup.setReplicaAllowed(Boolean.parseBoolean(replica));
            }
        } finally {
            wunlock();
        }
    }

    @Override
    public Map<String, SelectionPool> getPools() {
        rlock();
        try {
            return new HashMap<>(_pools);
        } finally {
            runlock();
        }
    }

    @Override
    public Map<String, SelectionLinkGroup> getLinkGroups() {
        rlock();
        try {
            return new HashMap<>(_linkGroups);
        } finally {
            runlock();
        }
    }

    @Override
    public LinkGroup getLinkGroupByName(String linkGroupName) {
        LinkGroup linkGroup = null;
        rlock();
        try {
            linkGroup = _linkGroups.get(linkGroupName);
        } finally {
            runlock();
        }
        return linkGroup;
    }

    @Override
    public Collection<SelectionPoolGroup> getPoolGroupsOfPool(String poolName) {
        rlock();
        try {
            Pool pool = _pools.get(poolName);
            if (pool != null) {
                return new ArrayList<>(pool._pGroupList.values());
            } else {
                throw new NoSuchElementException(poolName);
            }
        } finally {
            runlock();

        }
    }

    /**
     * Returns true if and only if the pool can stage the given file. That is the only case if the
     * file is located on an HSM connected to the pool.
     */
    private boolean poolCanStageFile(Pool pool, FileAttributes file) {
        boolean rc = false;
        if (file.getStorageInfo().locations().isEmpty()
              && pool.getHsmInstances().contains(file.getHsm())) {
            // This is for backwards compatibility until all info
            // extractors support URIs.
            rc = true;
        } else {
            for (URI uri : file.getStorageInfo().locations()) {
                if (pool.getHsmInstances().contains(uri.getAuthority())) {
                    rc = true;
                }
            }
        }
        LOGGER.debug("{}: matching hsm ({}) found?: {}", pool.getName(), file.getHsm(), rc);
        return rc;
    }

    @Override
    public Collection<SelectionPool> getPoolsByPoolGroup(String poolGroup)
          throws NoSuchElementException {
        PGroup group = _pGroups.get(poolGroup);
        if (group == null) {
            throw new NoSuchElementException("No such pool group: " + poolGroup);
        }
        return new ArrayList<>(group._poolList.values());
    }

    @Override
    public Collection<SelectionPool> getAllDefinedPools(boolean enabledOnly) {
        List<SelectionPool> pools = new ArrayList<>(_pools.size());
        rlock();
        try {
            for (Pool pool : _pools.values()) {
                if ((!enabledOnly) || pool.isEnabled()) {
                    pools.add(pool);
                }
            }
        } finally {
            runlock();
        }
        return pools;
    }

    /**
     * Returns a live view of pools whos name match the given pattern.
     */
    private Collection<Pool> getPools(Pattern pattern) {
        return Maps.filterKeys(_pools, Predicates.contains(pattern)).values();
    }

    /**
     * Returns a displayable string describing a quantity of pools.
     */
    private String poolCountDescriptionFor(int count) {
        switch (count) {
            case 0:
                return "No pools";
            case 1:
                return "One pool";
            default:
                return String.valueOf(count) + " pools";
        }
    }

    protected void wlock() {
        _psuWriteLock.lock();
    }

    protected void wunlock() {
        _psuWriteLock.unlock();
    }

    protected void rlock() {
        _psuReadLock.lock();
    }

    protected void runlock() {
        _psuReadLock.unlock();
    }

    public static final String hh_psu_add_link = "<link> <pool>|<pool group> # deprecated use 'psu addto link'";

    @AffectsSetup
    public String ac_psu_add_link_$_2(Args args) {
        addLink(args.argv(0), args.argv(1));
        return "Command was successful; please use 'psu addto link' next time.";
    }

    public static final String hh_psu_addto_link = "<link> <pool>|<pool group>";

    @AffectsSetup
    public String ac_psu_addto_link_$_2(Args args) {
        addLink(args.argv(0), args.argv(1));
        return "";
    }

    public static final String hh_psu_addto_pgroup = "<pool group> <pool|@pgroup>";

    @AffectsSetup
    public String ac_psu_addto_pgroup_$_2(Args args) {
        addToPoolGroup(args.argv(0), args.argv(1));
        return "";
    }

    public static final String hh_psu_addto_linkGroup = "<link group> <link>";

    @AffectsSetup
    public String ac_psu_addto_linkGroup_$_2(Args args) {
        addToLinkGroup(args.argv(0), args.argv(1));
        return "";
    }

    public static final String hh_psu_addto_ugroup = "<unit group> <unit>";

    @AffectsSetup
    public String ac_psu_addto_ugroup_$_2(Args args) {
        addToUnitGroup(args.argv(0),
              args.argv(1),
              args.hasOption("net"));
        return "";
    }

    public static final String hh_psu_clear_im_really_sure = "# don't use this command";

    @AffectsSetup
    public String ac_psu_clear_im_really_sure(Args args) {
        clear();
        return "Voila, now everthing is really gone";
    }

    public static final String hh_psu_create_link = "<link> <unit group> [...]";

    @AffectsSetup
    public String ac_psu_create_link_$_2_99(Args args) {
        String name = args.argv(0);
        args.shift();
        createLink(name, args.getArguments());
        return "";
    }

    public static final String hh_psu_create_linkGroup = "<group name> [-reset]";

    @AffectsSetup
    public String ac_psu_create_linkGroup_$_1(Args args) {
        createLinkGroup(args.argv(0), args.hasOption("reset"));
        return "";
    }

    public static final String hh_psu_create_pool = "<pool> [-noping] [-disabled]";

    @AffectsSetup
    public String ac_psu_create_pool_$_1(Args args) {
        createPool(args.argv(0),
              args.hasOption("noping"), args.hasOption("disabled"), args.hasOption("rdonly"));
        return "";
    }

    public static final String hh_psu_create_pgroup = "<pool group> [-resilient (deprecated) || -primary]";

    @AffectsSetup
    public String ac_psu_create_pgroup_$_1(Args args) {
        boolean primary = args.hasOption("resilient") || args.hasOption("primary");
        if (args.hasOption("dynamic")) {
            Map<String, String> tags = Splitter.on(',')
                  .omitEmptyStrings()
                  .trimResults()
                  .withKeyValueSeparator('=')
                  .split(args.getOption("tags", ""));
            createDynamicPoolGroup(args.argv(0), primary, tags);
        } else {
            createPoolGroup(args.argv(0), primary);
        }
        return "";
    }

    public static final String hh_psu_create_unit = "-net|-store|-dcache|-protocol <name>";

    public static final String fh_psu_create_unit =
          "NAME\n" +
                "\tpsu create unit\n\n" +
                "SYNOPSIS\n" +
                "\tpsu create unit UNITTYPE NAME\n\n" +
                "DESCRIPTION\n" +
                "\tCreates a new unit of the specified type.  A unit is a predicate\n" +
                "\tthat is used to select which pools are eligable for a specific user\n" +
                "\trequest (to read data from dCache or write data).  Units are\n" +
                "\tcombined in unit-groups; see psu create unitgroup for more details.\n\n" +
                "\tThe UNITTYPE is one of '-net', '-store', '-dcache' or '-protocol'\n" +
                "\tto create a network, store, dCache or protocol unit, respectively.\n\n" +
                "\tThe NAME of the unit describes which particular subset of user\n" +
                "\trequests will be selected; for example, a network unit with the\n" +
                "\tname '10.1.0.0/24' will select only those requests from a computer\n" +
                "\twith an IP address matching that subnet.\n\n" +
                "\tThe NAME for network units is either an IPv4 address, IPv6 address,\n" +
                "\tan IPv4 subnet or an IPv6 subnet.  Subnets may be written either\n" +
                "\tusing CIDR notation or as an IP address and netmask, joined by a\n" +
                "\t'/'.\n\n" +
                "\tThe NAME for store units has the form <StorageClass>@<HSM-type>.\n" +
                "\tBoth <StorageClass> and <HSM-type> may be replaced with a '*' to\n" +
                "\tmatch any value.  If the HSM-type is 'osm' then <StorageClass> is\n" +
                "\tconstructed by joining the store-name and store-group with a colon:\n" +
                "\t<StoreName>:<StoreGroup>@osm.\n\n" +
                "\tThe NAME for a dcache unit is an arbitrary string.  This matches\n" +
                "\tagainst the optional cache-class that may be set within the\n" +
                "\tnamespace in a similar fashion to the storage-class.\n\n" +
                "\tThe NAME for a protocol unit has the form <protocol>/<version>. If\n" +
                "\t<version> is '*' then all versions of that protocol match.\n\n" +
                "OPTIONS\n" +
                "\tnone\n";

    @AffectsSetup
    public String ac_psu_create_unit_$_1(Args args) {
        createUnit(args.argv(0),
              args.hasOption("net"),
              args.hasOption("store"),
              args.hasOption("dcache"),
              args.hasOption("protocol"));
        return "";
    }

    public static final String hh_psu_create_ugroup = "<unit group>";

    @AffectsSetup
    public String ac_psu_create_ugroup_$_1(Args args) {
        createUnitGroup(args.argv(0));
        return "";
    }

    public static final String hh_psu_dump_setup = "";

    public String ac_psu_dump_setup(Args args) {
        return dumpSetup();
    }

    public static final String hh_psu_ls_link = "[-l] [-a] [ <link> [...]]";

    public String ac_psu_ls_link_$_0_99(Args args) {
        boolean more = args.hasOption("a");
        boolean detail = args.hasOption("l") || more;
        return listPoolLinks(more, detail, args.getArguments());
    }

    public static final String hh_psu_ls_linkGroup
          = "[-l] [<link group1> ... <link groupN>]";

    public String ac_psu_ls_linkGroup_$_0_99(Args args) {
        return listLinkGroups(args.hasOption("l"),
              args.getArguments());
    }

    public static final String hh_psu_ls_netunits = "";

    public String ac_psu_ls_netunits(Args args) {
        return listNetUnits();
    }

    public static final String hh_psu_ls_pgroup = "[-l] [-a] [<pool group> [...]]";

    public String ac_psu_ls_pgroup_$_0_99(Args args) {
        boolean more = args.hasOption("a");
        boolean detail = args.hasOption("l") || more;
        return listPoolGroups(more, detail, args.getArguments());
    }

    public static final String hh_psu_ls_pool = "[-l] [-a] [<pool glob> [...]]";

    public String ac_psu_ls_pool_$_0_99(Args args) {
        boolean more = args.hasOption("a");
        boolean detail = args.hasOption("l") || more;
        return listPool(more, detail, args.getArguments());
    }

    public static final String hh_psu_ls_ugroup
          = "[-l] [-a] [<unit group> [...]]";

    public String ac_psu_ls_ugroup_$_0_99(Args args) {
        boolean more = args.hasOption("a");
        boolean detail = args.hasOption("l") || more;
        return listUnitGroups(more, detail, args.getArguments());
    }

    public static final String hh_psu_ls_unit = " [-a] [<unit> [...]]";

    public String ac_psu_ls_unit_$_0_99(Args args) {
        boolean more = args.hasOption("a");
        boolean detail = args.hasOption("l") || more;
        return listUnits(more, detail, args.getArguments());
    }

    public static final String hh_psu_match = "[-linkGroup=<link group>] [-pnfsId=<pnfsid>] "
          + "[-path=<path>] [-storageClass=<storage class>] [-hsm=<hsm>] [-cacheClass=<cache class>] "
          + "read|cache|write|p2p <client host>|* <protocol>|* ";

    public String ac_psu_match_$_3(Args args) throws Exception {
        return printPreferenceLevels((PoolPreferenceLevel[]) ac_psux_match_$_3(args));
    }

    public static final String hh_psu_match2 = "<unit> [...] [-net=<net unit>}";

    public String ac_psu_match2_$_1_99(Args args) {
        return matchUnits(args.getOpt("net"), args.getArguments());
    }

    public static final String hh_psu_netmatch = "<host address>";

    public String ac_psu_netmatch_$_1(Args args) throws UnknownHostException {
        return netMatch(args.argv(0));
    }

    public static final String hh_psu_removefrom_linkGroup
          = "<link group> <link>";

    @AffectsSetup
    public String ac_psu_removefrom_linkGroup_$_2(Args args) {
        removeFromLinkGroup(args.argv(0), args.argv(1));
        return "";
    }

    public static final String hh_psu_removefrom_pgroup = "<pool group> <pool|@pgroup>";

    @AffectsSetup
    public String ac_psu_removefrom_pgroup_$_2(Args args) {
        removeFromPoolGroup(args.argv(0), args.argv(1));
        return "";
    }

    public static final String hh_psu_removefrom_ugroup
          = "<unit group> <unit> -net";

    @AffectsSetup
    public String ac_psu_removefrom_ugroup_$_2(Args args) {
        removeFromUnitGroup(args.argv(0), args.argv(1),
              args.hasOption("net"));
        return "";
    }

    public static final String hh_psu_remove_link = "<link>";

    @AffectsSetup
    public String ac_psu_remove_link_$_1(Args args) {
        removeLink(args.argv(0));
        return "";
    }

    public static final String hh_psu_remove_linkGroup = "<link group>";

    @AffectsSetup
    public String ac_psu_remove_linkGroup_$_1(Args args) {
        removeLinkGroup(args.argv(0));
        return "";
    }

    public static final String hh_psu_remove_pool = "<pool>";

    @AffectsSetup
    public String ac_psu_remove_pool_$_1(Args args) {
        if (Glob.isGlob(args.argv(0))) {
            Glob glob = new Glob(args.argv(0));
            List<String> names = getPools(glob.toPattern()).stream()
                  .map((Pool pool) -> pool.getName()).collect(Collectors.toList());
            for (String name : names) {
                removePool(name);
            }
        } else {
            removePool(args.argv(0));
        }
        return "";
    }

    public static final String hh_psu_remove_pgroup = "<pool group>";

    @AffectsSetup
    public String ac_psu_remove_pgroup_$_1(Args args) {
        removePoolGroup(args.argv(0));
        return "";
    }

    public static final String hh_psu_remove_unit = "<unit> [-net]";

    @AffectsSetup
    public String ac_psu_remove_unit_$_1(Args args) {
        removeUnit(args.argv(0), args.hasOption("net"));
        return "";
    }

    public static final String hh_psu_remove_ugroup = "<unit group>";

    @AffectsSetup
    public String ac_psu_remove_ugroup_$_1(Args args) {
        removeUnitGroup(args.argv(0));
        return "";
    }


    public static final String hh_psu_set_active = "<pool>|* [-no]";

    public String ac_psu_set_active_$_1(Args args) {
        setPoolActive(args.argv(0), !args.hasOption("no"));
        return "";
    }

    public static final String hh_psu_set_allpoolsactive = "on|off";

    @AffectsSetup
    public String ac_psu_set_allpoolsactive_$_1(Args args) throws
          CommandSyntaxException {
        try {
            setAllPoolsActive(args.argv(0));
        } catch (IllegalArgumentException e) {
            throw new CommandSyntaxException(e.getMessage());
        }

        return "";
    }

    public static final String hh_psu_set_disabled = "<pool glob>";

    public String ac_psu_set_disabled_$_1(Args args) {
        return setPoolDisabled(args.argv(0));
    }

    public static final String hh_psu_set_enabled = "<pool glob>";

    public String ac_psu_set_enabled_$_1(Args args) {
        return setPoolEnabled(args.argv(0));
    }

    public static final String hh_psu_set_link
          = "<link> [-readpref=<readpref>] [-writepref=<writepref>] "
          + "[-cachepref=<cachepref>] [-p2ppref=<p2ppref>] "
          + "[-section=<section>|NONE]";

    @AffectsSetup
    public String ac_psu_set_link_$_1(Args args) {
        setLink(args.argv(0), args.getOption("readpref"),
              args.getOption("writepref"),
              args.getOption("cachepref"), args.getOption("p2ppref"),
              args.getOption("section"));
        return "";
    }


    public static final String hh_psu_set_linkGroup_custodialAllowed
          = "<link group> <true|false>";

    @AffectsSetup
    public String ac_psu_set_linkGroup_custodialAllowed_$_2(Args args) {
        setLinkGroup(args.argv(0), args.argv(1), null, null, null,
              null);
        return "";
    }

    public static final String hh_psu_set_linkGroup_nearlineAllowed
          = "<link group> <true|false>";

    @AffectsSetup
    public String ac_psu_set_linkGroup_nearlineAllowed_$_2(Args args) {
        setLinkGroup(args.argv(0), null, args.argv(1), null, null,
              null);
        return "";
    }

    public static final String hh_psu_set_linkGroup_onlineAllowed
          = "<link group> <true|false>";

    @AffectsSetup
    public String ac_psu_set_linkGroup_onlineAllowed_$_2(Args args) {
        setLinkGroup(args.argv(0), null, null, args.argv(1), null,
              null);
        return "";
    }

    public static final String hh_psu_set_linkGroup_outputAllowed
          = "<link group> <true|false>";

    @AffectsSetup
    public String ac_psu_set_linkGroup_outputAllowed_$_2(Args args) {
        setLinkGroup(args.argv(0), null, null, null, args.argv(1),
              null);
        return "";
    }

    public static final String hh_psu_set_linkGroup_replicaAllowed
          = "<link group> <true|false>";

    @AffectsSetup
    public String ac_psu_set_linkGroup_replicaAllowed_$_2(Args args) {
        setLinkGroup(args.argv(0), null, null, null, null, args.argv(1));
        return "";
    }

    public static final String hh_psu_set_pool =
          "<pool glob> enabled|disabled|ping|noping|rdonly|notrdonly";

    @AffectsSetup
    public String ac_psu_set_pool_$_2(Args args) {
        return setPool(args.argv(0), args.argv(1));
    }

    public static final String hh_psu_set_regex = "on | off";

    @AffectsSetup
    public String ac_psu_set_regex_$_1(Args args) {
        return setRegex(args.argv(0));
    }

    public void setPnfsHandler(PnfsHandler pnfsHandler) {
        _pnfsHandler = pnfsHandler;
    }

    @AffectsSetup
    @Command(name = "psu set storage unit",
          hint = "define resilience requirements for a storage unit",
          description = "Sets the required number of copies and/or "
                + "the partitioning of replicas by pool tags.")
    class StorageUnitCommand implements Callable<String> {

        @Option(name = "required",
              usage = "Set the number of copies required. "
                    + "Must be an integer >= 1.  A storage "
                    + "unit has required set to 1 by default.  "
                    + "Not specifying this attribute removes the "
                    + "required setting, making the unit non-resilient.")
        Integer required;

        @Option(name = "onlyOneCopyPer",
              separator = ",",
              usage = "A comma-delimited list of pool tag names used to "
                    + "partition copies across pools "
                    + "(interpreted as an 'and'-clause). "
                    + "A storage unit has an empty list by default.  "
                    + "Not specifying this attribute restores the default.")
        String[] onlyOneCopyPer;

        @Argument(usage = "Name of the storage unit.")
        String name;

        @Override
        public String call() throws IllegalArgumentException {
            Preconditions.checkArgument(required == null || required >= 1,
                  "required must be >= 1, "
                        + "was set to %s.",
                  required);
            setStorageUnit(name, required, onlyOneCopyPer);
            return "";
        }
    }

    public static final String hh_psu_unlink = "<link> <pool>|<pool group> # deprecated, use 'psu removefrom link'";

    @AffectsSetup
    public String ac_psu_unlink_$_2(Args args) throws CommandException {
        unlink(args.argv(0), args.argv(1));
        return "Command was successful; please use 'psu removefrom link' next time.";
    }

    public static final String hh_psu_removefrom_link = "<link> <pool>|<pool group>";

    @AffectsSetup
    public String ac_psu_removefrom_link_$_2(Args args) throws CommandException {
        unlink(args.argv(0), args.argv(1));
        return "";
    }

    /*
     * ***************************** PSUX **********************************
     */

    public static final String hh_psux_ls_link = "[<link>] [-x] [-resolve]";

    public Object ac_psux_ls_link_$_0_1(Args args) {
        String link = null;
        boolean resolve = args.hasOption("resolve");
        boolean isX = args.hasOption("x");
        if (args.argc() != 0) {
            link = args.argv(0);
        }

        return listLinkXml(isX, resolve, link);
    }

    public static final String hh_psux_ls_pgroup = "[<pool group>]";

    public Object ac_psux_ls_pgroup_$_0_1(Args args) {
        String groupName = args.argc() == 0 ? null : args.argv(0);
        return listPoolGroupXml(groupName);
    }

    public static final String hh_psux_ls_pool = "[<pool>]";

    public Object ac_psux_ls_pool_$_0_1(Args args) {
        String poolName = args.argc() == 0 ? null : args.argv(0);
        return listPoolXml(poolName);
    }

    public static final String hh_psux_ls_unit = "[<unit>]";

    public Object ac_psux_ls_unit_$_0_1(Args args) {
        String unitName = args.argc() == 0 ? null : args.argv(0);
        return listUnitXml(unitName);
    }

    public static final String hh_psux_ls_ugroup = "[<unit group>]";

    public Object ac_psux_ls_ugroup_$_0_1(Args args) {
        String groupName = args.argc() == 0 ? null : args.argv(0);
        return listUnitGroupXml(groupName);
    }

    public static final String hh_psux_match = "[-linkGroup=<link group>] [-pnfsId=<pnfsid>] "
          + "[-path=<path>] [-storageClass=<storage class>] [-hsm=<hsm>] [-cacheClass=<cache class>] "
          + "read|cache|write|p2p <client host>|* <protocol>|* ";

    public Object ac_psux_match_$_3(Args args)
          throws Exception {
        String linkGroup = args.getOpt("linkGroup");
        String pnfsid = args.getOpt("pnfsId");
        String path = args.getOpt("path");
        String storageClass = args.getOpt("storageClass");
        String hsm = args.getOpt("hsm");
        String cacheClass = args.getOpt("cacheClass");
        String direction = args.argv(0);
        String netUnit = args.argv(1);
        String protocolUnit = args.argv(2);

        FileAttributes storageAttributes = null;

        if (Strings.emptyToNull(pnfsid) != null) {
            if (direction.equalsIgnoreCase("WRITE")) {
                throw new CommandException("WRITE direction not allowed with -pnfsId.");
            }
            storageAttributes = _pnfsHandler.getFileAttributes(new PnfsId(pnfsid),
                  STORAGE_INFO);
        } else if (Strings.emptyToNull(path) != null) {
            if (direction.equalsIgnoreCase("WRITE")) {
                throw new CommandException("WRITE direction not allowed with -path.");
            }
            storageAttributes = _pnfsHandler.getFileAttributes(path, STORAGE_INFO);
        }

        if (storageAttributes != null) {
            cacheClass = storageAttributes.getCacheClass();
            storageClass = storageAttributes.getStorageClass();
            hsm = storageAttributes.getHsm();
        }

        if (Strings.emptyToNull(storageClass) == null) {
            throw new CommandException("storage class must be provided.");
        }

        if (Strings.emptyToNull(hsm) == null) {
            throw new CommandException("hsm must be provided.");
        }

        if (cacheClass == null) {
            cacheClass = "*";
        }

        String storageUnit = storageClass + "@" + hsm;

        return matchLinkGroupsXml(linkGroup, direction, storageUnit, cacheClass, netUnit,
              protocolUnit);
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        rlock();
        try {
            stream.defaultWriteObject();
        } finally {
            runlock();
        }
    }
}
