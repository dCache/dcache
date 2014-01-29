package diskCacheV111.poolManager;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.CommandSyntaxException;

import org.dcache.util.Args;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;

public class PoolSelectionUnitV2
    implements Serializable,
               PoolSelectionUnit,
               CellCommandListener,
               CellSetupProvider
{

    private static final String __version = "$Id: PoolSelectionUnitV2.java,v 1.42 2007-10-25 14:03:54 tigran Exp $";
    private static final Logger _log = LoggerFactory.getLogger(PoolSelectionUnitV2.class);
    private static final String NO_NET = "<no net>";

    @Override
    public String getVersion() {
        return __version;
    }

    public static final int STORE = 1;
    public static final int DCACHE = 2;
    public static final int NET = 3;
    public static final int PROTOCOL = 4;

    private static final long serialVersionUID = 4852540784324544199L;

    private final Map<String, PGroup> _pGroups = new HashMap<>();
    private final Map<String, Pool> _pools = new HashMap<>();
    private final Map<String, Link> _links = new HashMap<>();
    private final Map<String, LinkGroup> _linkGroups = new HashMap<>();
    private final Map<String, UGroup> _uGroups = new HashMap<>();
    private final Map<String, Unit> _units = new HashMap<>();
    private boolean _useRegex;
    private boolean _allPoolsActive;

    /**
     * Ok, this is the critical part of PoolManager, but (!!!) the whole select
     * path is READ-ONLY, unless we change setup. So ReadWriteLock is what we
     * are looking for, while is a point of serialization.
     */

    private final ReadWriteLock _psuReadWriteLock = new ReentrantReadWriteLock();
    private final Lock _psuReadLock = _psuReadWriteLock.readLock();
    private final Lock _psuWriteLock = _psuReadWriteLock.writeLock();

    private final NetHandler _netHandler = new NetHandler();

    @Override
    public Map<String, SelectionLink> getLinks() {
        _psuReadLock.lock();
        try {
            return Maps.<String, SelectionLink>newHashMap(_links);
        } finally {
            _psuReadLock.unlock();
        }
    }

    @Override
    public Map<String, SelectionUnit> getSelectionUnits() {
        _psuReadLock.lock();
        try {
            return Maps.<String, SelectionUnit>newHashMap(_units);
        } finally {
            _psuReadLock.unlock();
        }
    }

    @Override
    public Map<String, SelectionUnitGroup> getUnitGroups() {
        _psuReadLock.lock();
        try {
            return Maps.<String, SelectionUnitGroup>newHashMap(_uGroups);
        } finally {
            _psuReadLock.unlock();
        }
    }

    @Override
    public Collection<SelectionLink> getLinksPointingToPoolGroup(String poolGroup) throws NoSuchElementException {
        _psuReadLock.lock();
        try {
            PGroup group = _pGroups.get(poolGroup);
            if (group == null) {
                throw new NoSuchElementException("No such pool group: " + poolGroup);
            }
            return new ArrayList<SelectionLink>(group._linkList.values());
        } finally {
            _psuReadLock.unlock();
        }
    }

    @Override
    public SelectionLink getLinkByName(String name) throws NoSuchElementException {

        Link link = null;

        _psuReadLock.lock();
        try {
            link = _links.get(name);
            if (link == null) {
                throw new NoSuchElementException("Link not found : " + name);
            }
        } finally {
            _psuReadLock.unlock();
        }
        return link;
    }

    @Override
    public String[] getDefinedPools(boolean enabledOnly) {

        List<String> list = new ArrayList<>();
        _psuReadLock.lock();
        try {

            for (Pool pool : _pools.values()) {
                if ((!enabledOnly) || pool.isEnabled()) {
                    list.add(pool.getName());
                }
            }
        } finally {
            _psuReadLock.unlock();
        }
        return list.toArray(new String[list.size()]);
    }

    @Override
    public String[] getActivePools() {
        List<String> list = new ArrayList<>();

        _psuReadLock.lock();
        try {
            for (Pool pool : _pools.values()) {
                if (pool.isEnabled() && pool.isActive()) {
                    list.add(pool.getName());
                }
            }
        } finally {
            _psuReadLock.unlock();
        }
        return list.toArray(new String[list.size()]);
    }

    @Override
    public void beforeSetup()
    {
          clear();
    }

    @Override
    public void afterSetup()
    {
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        _psuReadLock.lock();
        try {

            pw.append("#\n# Printed by ").append(getClass().getName())
                    .append(" at ").append(new Date().toString()).append(
                            "\n#\n#\n");
            pw.append("psu set regex ").append(_useRegex?"on":"off").append("\n");
            pw.append("psu set allpoolsactive ").append(_allPoolsActive?"on":"off").append("\n");
            pw.append("#\n# The units ...\n#\n");
            for (Unit unit : _units.values()) {
                int type = unit.getType();
                pw.append("psu create unit ").append(
                        type == STORE ? "-store " : type == DCACHE ? "-dcache"
                                : type == PROTOCOL ? "-protocol" : "-net   ")
                        .append(" ").append(unit.getName()).append("\n");
            }
            pw.append("#\n# The unit Groups ...\n#\n");
            for (UGroup group : _uGroups.values()) {
                pw.append("psu create ugroup ").append(group.getName()).append(
                        "\n");
                for (Unit unit : group._unitList.values()) {
                    pw.append("psu addto ugroup ").append(group.getName())
                            .append(" ").append(unit.getName()).append("\n");
                }
            }
            pw.append("#\n# The pools ...\n#\n");
            for (Pool pool : _pools.values()) {
                pw.append("psu create pool ").append(pool.getName());

                if (!pool.isPing()) {
                    pw.append(" -noping");
                }
                if (!pool.isEnabled()) {
                    pw.append(" -disabled");
                }

                pw.append("\n");
            }
            pw.append("#\n# The pool groups ...\n#\n");
            for (PGroup group : _pGroups.values()) {
                pw.append("psu create pgroup ").append(group.getName()).append(
                        "\n");
                for (Pool pool : group._poolList.values()) {
                    pw.append("psu addto pgroup ").append(group.getName())
                            .append(" ").append(pool.getName()).append("\n");
                }
            }
            pw.append("#\n# The links ...\n#\n");
            for (Link link : _links.values()) {
                pw.append("psu create link ").append(link.getName());
                for (UGroup group : link._uGroupList.values()) {
                    pw.append(" ").append(group.getName());
                }
                pw.append("\n");
                pw.append("psu set link ").append(link.getName()).append(" ")
                        .println(link.getAttraction());
                for (PoolCore poolCore : link._poolList.values()) {
                    pw.append("psu add link ").append(link.getName()).append(
                            " ").println(poolCore.getName());
                }
            }

            pw.append("#\n# The link Groups ...\n#\n");
            for (LinkGroup linkGroup : _linkGroups.values()) {
                pw.append("psu create linkGroup ").append(linkGroup.getName());
                pw.append("\n");

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

                for (SelectionLink link : linkGroup.getLinks()) {
                    pw.append("psu addto linkGroup ").append(
                            linkGroup.getName()).append(" ").println(
                            link.getName());
                }
            }

        } finally {
            _psuReadLock.unlock();
        }
    }

    public void clear() {

        _psuWriteLock.lock();
        try {
            _netHandler.clear();
            _pGroups.clear();
            _pools.clear();
            _links.clear();
            _uGroups.clear();
            _units.clear();
            _linkGroups.clear();
        } finally {
            _psuWriteLock.unlock();
        }

    }

    public void setActive(String poolName, boolean active) {
        _psuWriteLock.lock();
        try {
            Pool pool = _pools.get(poolName);
            if (pool != null) {
                pool.setActive(active);
            }
        } finally {
            _psuWriteLock.unlock();
        }
    }

    public long getActive(String poolName) {

        long active = 100000000L;
        _psuReadLock.lock();
        try {
            Pool pool = _pools.get(poolName);
            if (pool != null) {
                active = pool.getActive();
            }
        } finally {
            _psuReadLock.unlock();
        }
        return active;
    }

    private int setEnabled(Glob glob, boolean enabled)
    {
        _psuWriteLock.lock();
        try {
            int count = 0;
            for (Pool pool: getPools(glob.toPattern())) {
                count++;
                pool.setEnabled(enabled);
            }
            return count;
        } finally {
            _psuWriteLock.unlock();
        }
    }

    public boolean isEnabled(String poolName) {

        boolean isEnabled = false;
        _psuReadLock.lock();
        try {
            Pool pool = _pools.get(poolName);
            if (pool != null) {
                isEnabled = pool.isEnabled();
            }
        } finally {
            _psuReadLock.unlock();
        }
        return isEnabled;
    }

    @Override
    public SelectionPool getPool(String poolName) {

        SelectionPool pool = null;

        _psuReadLock.lock();
        try {
            pool = _pools.get(poolName);
        } finally {
            _psuReadLock.unlock();
        }

        return pool;
    }

    @Override
    public SelectionPool getPool(String poolName, boolean create) {
        Pool pool = _pools.get(poolName);
        if ((pool != null) || !create) {
            return pool;
        }

        pool = new Pool(poolName);

        _psuReadLock.lock();
        try {
            _pools.put(pool.getName(), pool);
            PGroup group = _pGroups.get("default");
            if (group == null) {
                throw new IllegalArgumentException("Not found : " + "default");
            }

            //
            // shall we disallow more than one parent group ?
            //
            // if( pool._pGroupList.size() > 0 )
            // throw new
            // IllegalArgumentException( poolName +" already member" ) ;

            pool._pGroupList.put(group.getName(), group);
            group._poolList.put(pool.getName(), pool);
        } finally {
            _psuReadLock.unlock();
        }
        return pool;
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

    @Override
    public PoolPreferenceLevel[] match(DirectionType type,  String netUnitName, String protocolUnitName,
            FileAttributes fileAttributes, String linkGroupName) {

        StorageInfo storageInfo = fileAttributes.getStorageInfo();
        String storeUnitName = storageInfo.getStorageClass()+"@"+storageInfo.getHsm();
        String dCacheUnitName = storageInfo.getCacheClass();

        Map<String, String> variableMap = storageInfo.getMap();

        _log.debug("running match: type={} store={} dCacheUnit={} net={} protocol={} keys={} locations={} linkGroup={}",
                type, storeUnitName, dCacheUnitName, netUnitName, protocolUnitName,
                variableMap, storageInfo.locations(), linkGroupName);


        PoolPreferenceLevel[] result = null;
        _psuReadLock.lock();
        try {
            //
            // resolve the unit from the unitname (or net unit mask)
            //
            // regexp code added by rw2 12/5/02
            // original code is in the else
            //
            List<Unit> list = new ArrayList<>();
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
                //
                // If a pattern matches then use it, fail over to a class,
                // then universal. If nothing, throw exception
                //
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
                    int ind = storeUnitName.lastIndexOf("@");
                    if ((ind > 0) && (ind < (storeUnitName.length() - 1))) {
                        String template = "*@"
                                + storeUnitName.substring(ind + 1);
                        if ((unit = _units.get(template)) == null) {

                            if ((unit = _units.get("*@*")) == null) {
                                _log.debug("no matching storage unit found for: {}", storeUnitName);
                                throw new IllegalArgumentException(
                                        "Unit not found : " + storeUnitName);
                            }
                        }
                    } else {
                        throw new IllegalArgumentException(
                                "IllegalUnitFormat : " + storeUnitName);
                    }
                }
                _log.debug("matching storage unit found for: {}", storeUnitName);
                list.add(unit);
            }
            if (protocolUnitName != null) {

                Unit unit = findProtocolUnit(protocolUnitName);
                //
                if (unit == null){
                    _log.debug("no matching protocol unit found for: {}", protocolUnitName);
                    throw new IllegalArgumentException("Unit not found : "
                            + protocolUnitName);
                }
                _log.debug("matching protocol unit found: {}", unit);
                list.add(unit);
            }
            if (dCacheUnitName != null) {
                Unit unit = _units.get(dCacheUnitName);
                if (unit == null) {
                    _log.debug("no matching dCache unit found for: {}", dCacheUnitName);
                    throw new IllegalArgumentException("Unit not found : "
                            + dCacheUnitName);
                }
                _log.debug("matching dCache unit found: {}", unit);
                list.add(unit);
            }
            if (netUnitName != null) {
                try {
                    Unit unit = _netHandler.match(netUnitName);
                    if (unit == null) {
                        _log.debug("no matching net unit found for: {}", netUnitName);
                        throw new IllegalArgumentException(
                                "Unit not matched : " + netUnitName);
                    }
                    _log.debug("matching net unit found: {}" + unit);
                    list.add(unit);
                } catch (UnknownHostException uhe) {
                    throw new IllegalArgumentException(
                            "NetUnit not resolved : " + netUnitName);
                }
            }
            //
            // match the requests ( logical AND )
            //
            //
            // Map map = null ;
            // while( units.hasNext() )map = match( map , (Unit)units.next() ) ;
            // Iterator links = map.values().iterator() ;
            //

            //
            // i) sort according to the type (read,write,cache)
            // ii) the and is only OK if we have at least as many
            // units (from the arguments) as required by the
            // number of uGroupList(s).
            // iii) check for the hashtable if required.
            //
            int fitCount = list.size();
            Set<Link> sortedSet = new TreeSet<>(new LinkComparator(type));

            //
            // use subset on links if it's defined
            //

            LinkGroup linkGroup = null;
            if (linkGroupName != null) {
                linkGroup = _linkGroups.get(linkGroupName);
                if (linkGroup == null) {
                    _log.debug("LinkGroup not found : {}", linkGroupName );
                    throw new IllegalArgumentException("LinkGroup not found : "
                            + linkGroupName);
                }
            }

            //
            // find all links that matches the specified list of units
            //

            LinkMap matchingLinks = new LinkMap();
            for (Unit unit : list) {
                matchingLinks = match(matchingLinks, unit, linkGroup, type);
            }

            Iterator<Link> linkIterator = matchingLinks.iterator();
            while (linkIterator.hasNext()) {

                Link link = linkIterator.next();
                if ((link._uGroupList.size() <= fitCount)
                        && ((variableMap == null) || link.exec(variableMap))) {

                    sortedSet.add(link);
                }
            }
            int pref = -1;
            List<List<Link>> listList = new ArrayList<>();
            List<Link> current = null;

            switch (type) {

                case READ:
                    for (Link link : sortedSet) {
                        if (link.getReadPref() < 1) {
                            continue;
                        }
                        if (link.getReadPref() != pref) {
                            listList.add(current = new ArrayList<>());
                            pref = link.getReadPref();
                        }
                        current.add(link);
                    }
                    break;
                case CACHE:
                    for (Link link : sortedSet) {
                        if (link.getCachePref() < 1) {
                            continue;
                        }
                        if (link.getCachePref() != pref) {
                            listList.add(current = new ArrayList<>());
                            pref = link.getCachePref();
                        }
                        current.add(link);
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
                            listList.add(current = new ArrayList<>());
                            pref = tmpPref;
                        }
                        current.add(link);
                    }
                    break;
                case WRITE:
                    for (Link link : sortedSet) {
                        if (link.getWritePref() < 1) {
                            continue;
                        }
                        if (link.getWritePref() != pref) {
                            listList.add(current = new ArrayList<>());
                            pref = link.getWritePref();
                        }
                        current.add(link);
                    }
            }
            List<Link>[] x = listList.toArray(new List[listList.size()]);
            result = new PoolPreferenceLevel[x.length];
            //
            // resolve the links to the pools
            //
            for (int i = 0; i < x.length; i++) {

                List<Link> linkList = x[i];
                List<String> resultList = new ArrayList<>();
                String tag = null;

                for (Link link : linkList) {
                    //
                    // get the link if available
                    //
                    if ((tag == null) && (link.getTag() != null)) {
                        tag = link.getTag();
                    }

                    for (PoolCore poolCore : link._poolList.values()) {
                        if (poolCore instanceof Pool) {
                            Pool pool = (Pool) poolCore;
                            _log.debug("Pool: {} can read from tape? : {}", pool, pool.canReadFromTape());
                            if (((type == DirectionType.READ && pool.canRead())
                                 || (type == DirectionType.CACHE && pool.canReadFromTape()
                                     && poolCanStageFile(pool, storageInfo))
                                 || (type == DirectionType.WRITE && pool.canWrite())
                                 || (type == DirectionType.P2P && pool.canWriteForP2P()))
                                && (_allPoolsActive || pool.isActive())) {
                                resultList.add(pool.getName());
                            }
                        } else {
                            for (Pool pool : ((PGroup)poolCore)._poolList.values()) {
                                _log.debug("Pool: {} can read from tape? : {}", pool, pool.canReadFromTape());
                                if (((type == DirectionType.READ && pool.canRead())
                                     || (type == DirectionType.CACHE && pool.canReadFromTape()
                                         && poolCanStageFile(pool, storageInfo))
                                     || (type == DirectionType.WRITE && pool.canWrite())
                                     || (type == DirectionType.P2P && pool.canWriteForP2P()))
                                    && (_allPoolsActive || pool.isActive())) {
                                    resultList.add(pool.getName());
                                }
                            }
                        }
                    }
                }
                result[i] = new PoolPreferenceLevel(resultList, tag);
            }

        } finally {
            _psuReadLock.unlock();
        }

        if( _log.isDebugEnabled() ) {

            StringBuilder sb = new StringBuilder("match done: ");

            for( int i = 0; i < result.length; i++) {
                sb.append("[").append(i).append("] :");
                for(String poolName: result[i].getPoolList()) {
                    sb.append(" ").append(poolName);
                }
            }
            _log.debug(sb.toString());
        }
        return result;
    }

    @Override
    public String getProtocolUnit(String protocolUnitName) {
        Unit unit = findProtocolUnit(protocolUnitName);
        return unit == null ? null : unit.getName();
    }

    //
    // Legal formats : <protocol>/<version>
    //
    private boolean _protocolsChecked;

    public Unit findProtocolUnit(String protocolUnitName) {
        //
        if ((protocolUnitName == null) || (protocolUnitName.length() == 0)) {
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
        _psuReadLock.lock();
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
            _psuReadLock.unlock();
        }
        //
        return unit;

    }

    @Override
    public String getNetIdentifier(String address) throws UnknownHostException {

        _psuReadLock.lock();
        try {
            NetUnit unit = _netHandler.match(address);
	    if (unit == null) {
                return NO_NET;
            }
            return unit.getCanonicalName();
        } finally {
            _psuReadLock.unlock();
        }
    }

    /**
     * Picks links associated with a unit (elementary rule).
     *
     * @param unit
     *            The unit as the matching criteria
     * @param linkGroup
     *            Use only subset of links if defined, or all associated links
     *            if not defined (null)
     * @return the matching links
     */
    public Map<String, Link> match(Unit unit, LinkGroup linkGroup, DirectionType iotype) {

        Map<String, Link> map = new HashMap<>();

        _psuReadLock.lock();
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
                            _log.debug("link {} matching to unit {}", link.getName(), unit);
                            map.put(link.getName(), link);
                        }
                    } else if (linkGroup.contains(link)) {
                        //
                        // only take link if it is in the specified link group
                        //
                        _log.debug("link {} matching to unit {}", link.getName(), unit);
                        map.put(link.getName(), link);
                    }
                }
            }
        } finally {
            _psuReadLock.unlock();
        }
        return map;
    }

    public final static String hh_psu_set_allpoolsactive = "on|off";

    public String ac_psu_set_allpoolsactive_$_1(Args args) throws CommandSyntaxException {

        String mode = args.argv(0);

        _psuWriteLock.lock();
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
                throw new CommandSyntaxException("Syntax error");
            }
        } finally {
            _psuWriteLock.unlock();
        }

        return "";
    }

    public final static String hh_psu_netmatch = "<host address>";

    public String ac_psu_netmatch_$_1(Args args) throws UnknownHostException {

        NetUnit unit = null;

        _psuReadLock.lock();
        try {
            unit = _netHandler.match(args.argv(0));
        } finally {
            _psuReadLock.unlock();
        }
        if (unit == null) {
            throw new IllegalArgumentException("Host not a unit : "
                    + args.argv(0));
        }
        return unit.toString();
    }

    public final static String hh_psu_match = "[-linkGroup=<link group>] read|cache|write|p2p <store unit>|* <dCache unit>|* <net unit>|* <protocol unit>|* ";

    public String ac_psu_match_$_5(Args args) throws Exception {

        try {
            long start = System.currentTimeMillis();
            FileAttributes fileAttributes = new FileAttributes();
            fileAttributes.setStorageInfo(GenericStorageInfo.valueOf(args.argv(1), args.argv(2)));

            PoolPreferenceLevel[] list = match(args.argv(0).equals("*") ? DirectionType.ANY
                    : DirectionType.valueOf(args.argv(0).toUpperCase()),
                    args.argv(3).equals("*") ? null : args.argv(3), args
                            .argv(4).equals("*") ? null : args.argv(4), fileAttributes,
                    args.getOpt("linkGroup"));
            start = System.currentTimeMillis() - start;

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
            sb.append("(time used : ").append(start).append(" millis)\n");
            return sb.toString();
        } catch (Exception ee) {
            ee.printStackTrace();
            throw ee;
        }
    }

    public final static String hh_psu_match2 = "<unit> [...] [-net=<net unit>}";

    public String ac_psu_match2_$_1_99(Args args)
    {
        StringBuilder sb = new StringBuilder();
        Map<String, Link> map = null;
        int required = args.argc();

        _psuReadLock.lock();
        try {
            for (int i = 0; i < args.argc(); i++) {
                String unitName = args.argv(i);
                Unit unit = _units.get(unitName);
                if (unit == null) {
                    throw new IllegalArgumentException("Unit not found : "
                            + unitName);
                }
                // TODO:
                map = match(map, unit, DirectionType.READ);
            }
            String netUnitName = args.getOpt("net");
            if (netUnitName != null) {
                Unit unit = _netHandler.find(new NetUnit(netUnitName));
                if (unit == null) {
                    throw new IllegalArgumentException(
                            "Unit not found in netList : " + netUnitName);
                }
                // TODO:
                map = match(map, unit, DirectionType.READ);
            }
            for (Link link : map.values()) {
                if (link._uGroupList.size() != required) {
                    continue;
                }
                sb.append("Link : ").append(link.toString()).append("\n");

                for(SelectionPool pool: link.getPools()) {
                    sb.append("    ").append(pool.getName()).append(
                            "\n");
                }
            }

        } finally {
            _psuReadLock.unlock();
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
    public final static String hh_psu_create_pgroup = "<pool group>";

    public String ac_psu_create_pgroup_$_1(Args args) {
        String name = args.argv(0);

        _psuWriteLock.lock();
        try {
            if (_pGroups.get(name) != null) {
                throw new IllegalArgumentException("Duplicated entry : " + name);
            }

            PGroup group = new PGroup(name);

            _pGroups.put(group.getName(), group);
        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_set_regex = "on | off";

    public String ac_psu_set_regex_$_1(Args args) {
        String retVal;
        String onOff = args.argv(0);
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

    public final static String hh_psu_create_pool = "<pool> [-noping]";

    public String ac_psu_create_pool_$_1(Args args) {
        String name = args.argv(0);

        _psuWriteLock.lock();
        try {
            if (_pools.get(name) != null) {
                throw new IllegalArgumentException("Duplicated entry : " + name);
            }

            Pool pool = new Pool(name);
            if (args.hasOption("noping")) {
                pool.setPing(false);
            }
            if (args.hasOption("disabled")) {
                pool.setEnabled(false);
            }
            _pools.put(pool.getName(), pool);
        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_set_pool =
        "<pool glob> enabled|disabled|ping|noping|rdonly|notrdonly";
    public String ac_psu_set_pool_$_2(Args args)
    {
        Pattern pattern = Glob.parseGlobToPattern(args.argv(0));
        String mode = args.argv(1);

        _psuWriteLock.lock();
        try {
            int count = 0;
            for (Pool pool: getPools(pattern)) {
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
            _psuWriteLock.unlock();
        }
    }

    public final static String hh_psu_set_enabled = "<pool glob>";
    public String ac_psu_set_enabled_$_1(Args args)
    {
        int count = setEnabled(new Glob(args.argv(0)), true);
        return poolCountDescriptionFor(count) + " enabled";
    }

    public final static String hh_psu_set_disabled = "<pool glob>";
    public String ac_psu_set_disabled_$_1(Args args)
    {
        int count = setEnabled(new Glob(args.argv(0)), false);
        return poolCountDescriptionFor(count) + " disabled";
    }

    public final static String hh_psu_create_link = "<link> <unit group> [...]";
    public String ac_psu_create_link_$_2_99(Args args) {

        String name = args.argv(0);

        _psuWriteLock.lock();
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
            for (int i = 1; i < args.argc(); i++) {
                String uGroupName = args.argv(i);

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
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_create_ugroup = "<unit group>";

    public String ac_psu_create_ugroup_$_1(Args args) {
        String name = args.argv(0);

        _psuWriteLock.lock();
        try {
            if (_uGroups.get(name) != null) {
                throw new IllegalArgumentException("Duplicated entry : " + name);
            }

            UGroup group = new UGroup(name);

            _uGroups.put(group.getName(), group);
        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_create_unit = "-net|-store|-dcache|-protocol <name>";
    public static final String fh_psu_create_unit =
            "NAME\n"+
            "\tpsu create unit\n\n"+
            "SYNOPSIS\n"+
            "\tpsu create unit UNITTYPE NAME\n\n"+
            "DESCRIPTION\n"+
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
            "\tusing CIDR notation or as an IP address and netmask, joined by a\n"+
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
            "OPTIONS\n"+
            "\tnone\n";
    public String ac_psu_create_unit_$_1(Args args)
    {
        String name = args.argv(0);
        Unit unit = null;
        _psuWriteLock.lock();
        try {
            if (args.hasOption("net")) {
                NetUnit net = new NetUnit(name);
                _netHandler.add(net);
                unit = net;
            } else if (args.hasOption("store")) {
                unit = new Unit(name, STORE);
            } else if (args.hasOption("dcache")) {
                unit = new Unit(name, DCACHE);
            } else if (args.hasOption("protocol")) {
                unit = new ProtocolUnit(name);
            }
            if (unit == null) {
                throw new IllegalArgumentException(
                        "Unit type missing net/store/dcache/protocol");
            }

            String canonicalName = name; // will use the input name
            if (_units.get(canonicalName) != null) {
                throw new IllegalArgumentException("Duplicated entry : "
                        + canonicalName);
            }

            _units.put(canonicalName, unit);
        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_create_linkGroup = "<group name> [-reset]";

    public String ac_psu_create_linkGroup_$_1(Args args) {

        String newGroupName = args.argv(0);
        boolean reset = args.hasOption("reset");

        _psuWriteLock.lock();
        try {

            if (_linkGroups.containsKey(newGroupName) && !reset) {
                throw new IllegalArgumentException(
                        "LinkGroup already exists : " + newGroupName);
            }

            LinkGroup newGroup = new LinkGroup(newGroupName);
            _linkGroups.put(newGroupName, newGroup);
        } finally {
            _psuWriteLock.unlock();
        }

        return "";
    }

    //
    // ..................................................................
    //
    // the 'psux ... ls'
    //
    public final static String hh_psux_ls_pool = "[<pool>]";

    public Object ac_psux_ls_pool_$_0_1(Args args)
    {

        Object xlsResult = null;
        _psuReadLock.lock();
        try {
            if (args.argc() == 0) {
                xlsResult = _pools.keySet().toArray();
            } else {
                String poolName = args.argv(0);
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
            _psuReadLock.unlock();
        }

        return xlsResult;
    }

    public final static String hh_psux_ls_pgroup = "[<pool group>]";

    public Object ac_psux_ls_pgroup_$_0_1(Args args)
    {

        Object xlsResult = null;
        _psuReadLock.lock();
        try {

            if (args.argc() == 0) {
                xlsResult = _pGroups.keySet().toArray();
            } else {

                String groupName = args.argv(0);
                PGroup group = _pGroups.get(groupName);
                if (group == null) {
                    throw new IllegalArgumentException("Not found : "
                            + groupName);
                }

                Object[] result = new Object[3];
                result[0] = groupName;
                result[1] = group._poolList.keySet().toArray();
                result[2] = group._linkList.keySet().toArray();
                xlsResult = result;
            }
        } finally {
            _psuReadLock.unlock();
        }

        return xlsResult;
    }

    @Override
    public Map<String, SelectionPoolGroup> getPoolGroups() {
        _psuReadLock.lock();
        try {
            return Maps.<String, SelectionPoolGroup>newHashMap(_pGroups);
        } finally {
            _psuReadLock.unlock();
        }
    }

    public final static String hh_psux_ls_unit = "[<unit>]";

    public Object ac_psux_ls_unit_$_0_1(Args args)
    {

        Object xlsResult = null;
        _psuReadLock.lock();
        try {

            if (args.argc() == 0) {
                xlsResult = _units.keySet().toArray();
            } else {
                String unitName = args.argv(0);
                Unit unit = _units.get(unitName);
                if (unit == null) {
                    throw new IllegalArgumentException("Not found : "
                            + unitName);
                }

                Object[] result = new Object[3];
                result[0] = unitName;
                result[1] = unit.getType() == STORE ? "Store"
                        : unit.getType() == PROTOCOL ? "Protocol"
                                : unit.getType() == DCACHE ? "dCache"
                                        : unit.getType() == NET ? "Net" : "Unknown";
                result[2] = unit._uGroupList.keySet().toArray();
                xlsResult = result;
            }
        } finally {
            _psuReadLock.unlock();
        }

        return xlsResult;
    }

    public final static String hh_psux_ls_ugroup = "[<unit group>]";

    public Object ac_psux_ls_ugroup_$_0_1(Args args)
    {

        Object xlsResult = null;
        _psuReadLock.lock();
        try {
            if (args.argc() == 0) {
                xlsResult = _uGroups.keySet().toArray();
            } else {
                String groupName = args.argv(0);
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
            _psuReadLock.unlock();
        }

        return xlsResult;
    }

    public final static String hh_psux_ls_link = "[<link>] [-x] [-resolve]";

    public Object ac_psux_ls_link_$_0_1(Args args)
    {

        boolean resolve = args.hasOption("resolve");
        Object xlsResult = null;
        _psuReadLock.lock();
        try {

            if (args.argc() == 0) {
                if (!args.hasOption("x")) {
                    xlsResult = _links.keySet().toArray();
                } else {
                    List<Object[]> array = new ArrayList<>();
                    for (Link link : _links.values()) {
                        array.add(fillLinkProperties(link, resolve));
                    }
                    xlsResult = array;
                }
            } else {
                String linkName = args.argv(0);
                Link link = _links.get(linkName);
                if (link == null) {
                    throw new IllegalArgumentException("Not found : "
                            + linkName);
                }

                xlsResult = fillLinkProperties(link, resolve);
            }
        } finally {
            _psuReadLock.unlock();
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

    public final static String hh_psux_match = "[-linkGroup=<link group>] read|cache|write <store unit> <dCache unit> <net unit> <protocol unit>";

    public Object ac_psux_match_$_5(Args args)
    {
        FileAttributes fileAttributes = new FileAttributes();
        fileAttributes.setStorageInfo(GenericStorageInfo.valueOf(args.argv(1), args.argv(2)));

        PoolPreferenceLevel[] list = match(DirectionType.valueOf(args.argv(0).toUpperCase()),
                args.argv(3).equals("*") ? null : args.argv(3),
                args.argv(4).equals("*") ? null : args.argv(4), fileAttributes, args.getOpt("linkGroup"));
        return list;
    }

    // ..................................................................
    //
    // the 'ls'
    //
    public final static String hh_psu_ls_pool =
        "[-l] [-a] [<pool glob> [...]]";
    public String ac_psu_ls_pool_$_0_99(Args args)
    {
        StringBuilder sb = new StringBuilder();
        boolean more = args.hasOption("a");
        boolean detail = args.hasOption("l") || more;

        _psuReadLock.lock();
        try {
            Collection<Pool> pools;
            if (args.argc() == 0) {
                pools = _pools.values();
            } else {
                pools = new ArrayList<>();
                for (int n = 0; n < args.argc(); n++) {
                    pools.addAll(getPools(Glob.parseGlobToPattern(args.argv(n))));
                }
            }
            for (Pool pool: pools) {
                if (!detail) {
                    sb.append(pool.getName()).append("\n");
                } else {
                    sb.append(pool).append("\n");
                    sb.append(" linkList   :\n");
                    for (Link link: pool._linkList.values()) {
                        sb.append("   ").append(link).append("\n");
                    }
                    if (more) {
                        sb.append(" pGroupList : \n");
                        for (PGroup group: pool._pGroupList.values()) {
                            sb.append("   ").append(group).append("\n");
                        }
                    }
                }
            }
        } finally {
            _psuReadLock.unlock();
        }
        return sb.toString();
    }

    public final static String hh_psu_ls_pgroup = "[-l] [-a] [<pool group> [...]]";

    public String ac_psu_ls_pgroup_$_0_99(Args args) {
        StringBuilder sb = new StringBuilder();
        boolean more = args.hasOption("a");
        boolean detail = args.hasOption("l") || more;

        _psuReadLock.lock();
        try {
            Iterator<PGroup> i;
            if (args.argc() == 0) {
                i = _pGroups.values().iterator();
            } else {
                ArrayList<PGroup> l = new ArrayList<>();
                for (int n = 0; n < args.argc(); n++) {
                    PGroup o = _pGroups.get(args.argv(n));
                    if (o != null) {
                        l.add(o);
                    }
                }
                i = l.iterator();
            }
            while (i.hasNext()) {
                PGroup group = i.next();
                sb.append(group.getName()).append("\n");
                if (detail) {
                    sb.append(" linkList :\n");
                    for (Link link : group._linkList.values()) {
                        sb.append("   ").append(link.toString()).append(
                                "\n");
                    }
                    sb.append(" poolList :\n");
                    for (Pool pool : group._poolList.values()) {
                        sb.append("   ").append(pool.toString()).append("\n");
                    }
                }
            }
        } finally {
            _psuReadLock.unlock();
        }
        return sb.toString();
    }

    public final static String hh_psu_ls_link = "[-l] [-a] [ <link> [...]]";

    public String ac_psu_ls_link_$_0_99(Args args) {

        StringBuilder sb = new StringBuilder();
        boolean more = args.hasOption("a");
        boolean detail = args.hasOption("l") || more;

        _psuReadLock.lock();
        try {
            Iterator<Link> i;
            if (args.argc() == 0) {
                i = _links.values().iterator();
            } else {
                ArrayList<Link> l = new ArrayList<>();
                for (int n = 0; n < args.argc(); n++) {
                    Link o = _links.get(args.argv(n));
                    if (o != null) {
                        l.add(o);
                    }
                }
                i = l.iterator();
            }
            while (i.hasNext()) {
                Link link = i.next();
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
                    for (UGroup group : link._uGroupList.values()) {
                        sb.append("   ").append(group.toString()).append("\n");
                    }
                    if (more) {
                        sb.append(" poolList  :\n");
                        for (PoolCore core : link._poolList.values()) {
                            sb.append("   ").append(core.toString()).append(
                                    "\n");
                        }
                    }
                }
            }
        } finally {
            _psuReadLock.unlock();
        }
        return sb.toString();
    }

    public final static String hh_psu_ls_ugroup = "[-l] [-a] [<unit group> [...]]";

    public String ac_psu_ls_ugroup_$_0_99(Args args) {

        StringBuilder sb = new StringBuilder();
        boolean more = args.hasOption("a");
        boolean detail = args.hasOption("l") || more;

        _psuReadLock.lock();
        try {
            Iterator<UGroup> i;
            if (args.argc() == 0) {
                i = _uGroups.values().iterator();
            } else {
                ArrayList<UGroup> l = new ArrayList<>();
                for (int n = 0; n < args.argc(); n++) {
                    UGroup o = _uGroups.get(args.argv(n));
                    if (o != null) {
                        l.add(o);
                    }
                }
                i = l.iterator();
            }
            while (i.hasNext()) {
                UGroup group = i.next();
                sb.append(group.getName()).append("\n");
                if (detail) {
                    sb.append(" unitList :\n");
                    for (Unit unit : group._unitList.values()) {
                        sb.append("   ").append(unit.toString()).append("\n");
                    }
                    if (more) {
                        sb.append(" linkList :\n");
                        for (Link link : group._linkList.values()) {
                            sb.append("   ").append(link.toString()).append(
                                    "\n");
                        }
                    }
                }
            }
        } finally {
            _psuReadLock.unlock();
        }
        return sb.toString();
    }

    public final static String hh_psu_ls_netunits = "";

    public String ac_psu_ls_netunits(Args args) {
        StringBuilder sb = new StringBuilder();

        _psuReadLock.lock();
        try {
            for (int i = 0; i < _netHandler._netList.length; i++) {
                Map<Long, NetUnit> map = _netHandler._netList[i];
                if (map == null) {
                    continue;
                }
                String stringMask = _netHandler.bitsToString(i);
                sb.append(stringMask).append("/").append(i).append("\n");
                for (NetUnit net : map.values()) {
                    sb.append("   ").append(net.getHostAddress().getHostName());
                    if (i > 0) {
                        sb.append("/").append(stringMask);
                    }
                    sb.append("\n");
                }

            }
        } finally {
            _psuReadLock.unlock();
        }
        return sb.toString();
    }

    public final static String hh_psu_ls_unit = " [-a] [<unit> [...]]";

    public String ac_psu_ls_unit_$_0_99(Args args) {
        StringBuilder sb = new StringBuilder();
        boolean more = args.hasOption("a");
        boolean detail = args.hasOption("l") || more;

        _psuReadLock.lock();
        try {
            Iterator<Unit> i;
            if (args.argc() == 0) {
                i = _units.values().iterator();
            } else {
                ArrayList<Unit> l = new ArrayList<>();
                for (int n = 0; n < args.argc(); n++) {
                    Unit o = _units.get(args.argv(n));
                    if (o != null) {
                        l.add(o);
                    }
                }
                i = l.iterator();
            }
            while (i.hasNext()) {
                Unit unit = i.next();
                if (detail) {
                    sb.append(unit.toString()).append("\n");
                    if (more) {
                        sb.append(" uGroupList :\n");
                        for (UGroup group : unit._uGroupList.values()) {
                            sb.append("   ").append(group.toString()).append(
                                    "\n");
                        }
                    }
                } else {
                    sb.append(unit.getName()).append("\n");
                }
            }
        } finally {
            _psuReadLock.unlock();
        }
        return sb.toString();
    }

    public final static String hh_psu_ls_linkGroup = "[-l] [<link group1> ... <link groupN>]";

    public String ac_psu_ls_linkGroup_$_0_99(Args args) {

        StringBuilder sb = new StringBuilder();

        boolean isLongOutput = args.hasOption("l");
        _psuReadLock.lock();
        try {

            if (args.argc() != 0) {
                int count = args.argc();
                for (int i = 0; i < count; i++) {
                    LinkGroup linkGroup = _linkGroups.get(args.argv(i));
                    if (linkGroup == null) {
                        throw new IllegalArgumentException(
                                "LinkGroup not found : " + args.argv(i));
                    }

                    if (isLongOutput) {
                        sb.append(linkGroup).append("\n");
                    } else {
                        sb.append(args.argv(i)).append("\n");
                    }

                }

            } else {
                Set<String> allGroups = _linkGroups.keySet();
                for (String groupName : allGroups) {
                    LinkGroup linkGroup = _linkGroups.get(groupName);
                    if (linkGroup == null) {
                        throw new IllegalArgumentException(
                                "LinkGroup not found : " + groupName);
                    }

                    if (isLongOutput) {
                        sb.append(linkGroup).append("\n");
                    } else {
                        sb.append(groupName).append("\n");
                    }
                }
            }

        } finally {
            _psuReadLock.unlock();
        }

        return sb.toString();
    }

    public final static String hh_psu_dump_setup = "";
    public String ac_psu_dump_setup(Args args)
    {
        StringWriter s = new StringWriter();
        printSetup(new PrintWriter(s));
        return s.toString();
    }

    //
    // .............................................................................
    //
    // the 'removes'
    //
    public final static String hh_psu_remove_unit = "<unit> [-net]";

    public String ac_psu_remove_unit_$_1(Args args)
    {
        String unitName = args.argv(0);

        _psuWriteLock.lock();
        try {
            if (args.hasOption("net")) {
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

            if (unit instanceof NetUnit) {
                _netHandler.remove((NetUnit) unit);
            }

            for (UGroup group : unit._uGroupList.values()) {
                group._unitList.remove(unit.getCanonicalName());
            }

            _units.remove(unitName);
        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_remove_ugroup = "<unit group>";

    public String ac_psu_remove_ugroup_$_1(Args args) {
        String groupName = args.argv(0);

        _psuWriteLock.lock();
        try {
            UGroup group = _uGroups.get(groupName);
            if (group == null) {
                throw new IllegalArgumentException("UGroup not found : "
                        + groupName);
            }

            if (group._unitList.size() > 0) {
                throw new IllegalArgumentException("UGroup not empty : "
                        + groupName);
            }

            if (group._linkList.size() > 0) {
                throw new IllegalArgumentException(
                        "Still link(s) pointing to us : " + groupName);
            }

            _uGroups.remove(groupName);

        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_remove_pgroup = "<pool group>";

    public String ac_psu_remove_pgroup_$_1(Args args) {
        String name = args.argv(0);

        _psuWriteLock.lock();
        try {
            PGroup group = _pGroups.get(name);
            if (group == null) {
                throw new IllegalArgumentException("PGroup not found : " + name);
            }

            //
            // check if empty
            //
            if (group._poolList.size() != 0) {
                throw new IllegalArgumentException("PGroup not empty : " + name);
            }
            //
            // remove the links
            //
            PoolCore core = group;
            for (Link link : core._linkList.values()) {
                link._poolList.remove(core.getName());
            }
            //
            // remove from global
            //
            _pGroups.remove(name);
        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_remove_pool = "<pool>";

    public String ac_psu_remove_pool_$_1(Args args) {
        String name = args.argv(0);

        _psuWriteLock.lock();
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
            PoolCore core = pool;
            for (Link link : core._linkList.values()) {
                link._poolList.remove(core.getName());
            }
            //
            // remove from global
            //
            _pools.remove(name);
        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_removefrom_ugroup = "<unit group> <unit> -net";

    public String ac_psu_removefrom_ugroup_$_2(Args args)
    {
        String groupName = args.argv(0);
        String unitName = args.argv(1);

        _psuWriteLock.lock();
        try {
            UGroup group = _uGroups.get(groupName);
            if (group == null) {
                throw new IllegalArgumentException("UGroup not found : "
                        + groupName);
            }

            if (args.hasOption("net")) {
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
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_removefrom_pgroup = "<pool group> <pool>";

    public String ac_psu_removefrom_pgroup_$_2(Args args) {
        String groupName = args.argv(0);
        String poolName = args.argv(1);

        _psuWriteLock.lock();
        try {
            Pool pool = _pools.get(poolName);
            if (pool == null) {
                throw new IllegalArgumentException("Pool not found : "
                        + poolName);
            }

            PGroup group = _pGroups.get(groupName);
            if (group == null) {
                throw new IllegalArgumentException("PGroup not found : "
                        + groupName);
            }

            if (group._poolList.get(poolName) == null) {
                throw new IllegalArgumentException(poolName + " not member of "
                        + groupName);
            }

            group._poolList.remove(poolName);
            pool._pGroupList.remove(groupName);
        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_removefrom_linkGroup = "<link group> <link>";

    public String ac_psu_removefrom_linkGroup_$_2(Args args) {

        String linkGroupName = args.argv(0);
        String linkName = args.argv(1);

        _psuWriteLock.lock();
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
            _psuWriteLock.unlock();
        }

        return "";

    }

    public final static String hh_psu_remove_linkGroup = "<link group>";

    public String ac_psu_remove_linkGroup_$_1(Args args) {

        String linkGroupName = args.argv(0);

        _psuWriteLock.lock();
        try {

            LinkGroup linkGroup = _linkGroups.remove(linkGroupName);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                        + linkGroupName);
            }

            for (SelectionLink link : linkGroup.getAllLinks()) {
                if (link instanceof Link) {
                    ((Link) link).setLinkGroup(null);
                }
            }

        } finally {
            _psuWriteLock.unlock();
        }

        return "";
    }

    public final static String hh_psu_remove_link = "<link>";

    public String ac_psu_remove_link_$_1(Args args) {
        String name = args.argv(0);

        _psuWriteLock.lock();
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
            _psuWriteLock.unlock();
        }
        return "";
    }

    //
    // ........................................................................
    //
    // relations
    //
    public final static String hh_psu_addto_pgroup = "<pool group> <pool>";

    public String ac_psu_addto_pgroup_$_2(Args args) {
        String pGroupName = args.argv(0);
        String poolName = args.argv(1);

        // no lock required, while method does it
        addtoPoolGroup(pGroupName, poolName);

        return "";
    }

    private void addtoPoolGroup(String pGroupName, String poolName) throws IllegalArgumentException {

        _psuWriteLock.lock();
        try {
            PGroup group = _pGroups.get(pGroupName);
            if (group == null) {
                throw new IllegalArgumentException("Not found : " + pGroupName);
            }
            Pool pool = _pools.get(poolName);
            if (pool == null) {
                throw new IllegalArgumentException("Not found : " + poolName);
            }
            //
            // shall we disallow more than one parent group ?
            //
            // if( pool._pGroupList.size() > 0 )
            // throw new
            // IllegalArgumentException( poolName +" already member" ) ;

            pool._pGroupList.put(group.getName(), group);
            group._poolList.put(pool.getName(), pool);
        } finally {
            _psuWriteLock.unlock();
        }
    }

    public final static String hh_psu_addto_ugroup = "<unit group> <unit>";

    public String ac_psu_addto_ugroup_$_2(Args args)
    {

        String uGroupName = args.argv(0);
        String unitName = args.argv(1);

        _psuWriteLock.lock();
        try {
            if (args.hasOption("net")) {
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
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_addto_linkGroup = "<link group> <link>";

    public String ac_psu_addto_linkGroup_$_2(Args args) {
        String linkGroupName = args.argv(0);
        String linkName = args.argv(1);

        _psuWriteLock.lock();
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
            _psuWriteLock.unlock();
        }

        return "";
    }

    public final static String hh_psu_unlink = "<link> <pool>|<pool group>";

    public String ac_psu_unlink_$_2(Args args) {
        String linkName = args.argv(0);
        String poolName = args.argv(1);

        _psuWriteLock.lock();
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
            _psuWriteLock.unlock();
        }

        return "";
    }

    public final static String hh_psu_add_link = "<link> <pool>|<pool group>";

    public String ac_psu_add_link_$_2(Args args) {
        String linkName = args.argv(0);
        String poolName = args.argv(1);

        _psuWriteLock.lock();
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

            core._linkList.put(link.getName(), link);
            link._poolList.put(core.getName(), core);
        } finally {
            _psuWriteLock.unlock();
        }

        return "";
    }

    public final static String hh_psu_set_active = "<pool>|* [-no]";

    public String ac_psu_set_active_$_1(Args args) {
        String poolName = args.argv(0);
        boolean active = !args.hasOption("no");

        _psuWriteLock.lock();

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
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_set_link = "<link> [-readpref=<readpref>] [-writepref=<writepref>] [-cachepref=<cachepref>] [-p2ppref=<p2ppref>] [-section=<section>|NONE]";

    public String ac_psu_set_link_$_1(Args args) {

        String linkName = args.argv(0);
        _psuWriteLock.lock();

        try {
            Link link = _links.get(linkName);
            if (link == null) {
                throw new IllegalArgumentException("Not found : " + linkName);
            }

            String tmp = args.getOpt("readpref");
            if (tmp != null) {
                link.setReadPref(Integer.parseInt(tmp));
            }
            tmp = args.getOpt("cachepref");
            if (tmp != null) {
                link.setCachePref(Integer.parseInt(tmp));
            }
            tmp = args.getOpt("writepref");
            if (tmp != null) {
                link.setWritePref(Integer.parseInt(tmp));
            }
            tmp = args.getOpt("p2ppref");
            if (tmp != null) {
                link.setP2pPref(Integer.parseInt(tmp));
            }
            tmp = args.getOpt("section");
            if (tmp != null) {
                link.setTag(tmp.equals("NONE") ? null : tmp);
            }

        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    @Deprecated // Remove in 2.10
    public String ac_psu_set_linkGroup_attribute_$_2(Args args) {
        return "psu set linkGroup attribute is obsolete.";
    }


    public final static String hh_psu_set_linkGroup_custodialAllowed = "<link group> <true|false>";

    public String ac_psu_set_linkGroup_custodialAllowed_$_2(Args args) {

        _psuWriteLock.lock();

        try {
            String linkGroupName = args.argv(0);

            LinkGroup linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                        + linkGroupName);
            }

            linkGroup.setCustodialAllowed(Boolean.parseBoolean(args.argv(1)));

        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_set_linkGroup_outputAllowed = "<link group> <true|false>";

    public String ac_psu_set_linkGroup_outputAllowed_$_2(Args args) {

        _psuWriteLock.lock();

        try {
            String linkGroupName = args.argv(0);

            LinkGroup linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                        + linkGroupName);
            }

            linkGroup.setOutputAllowed(Boolean.parseBoolean(args.argv(1)));

        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_set_linkGroup_replicaAllowed = "<link group> <true|false>";

    public String ac_psu_set_linkGroup_replicaAllowed_$_2(Args args) {

        _psuWriteLock.lock();

        try {
            String linkGroupName = args.argv(0);

            LinkGroup linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                        + linkGroupName);
            }

            linkGroup.setReplicaAllowed(Boolean.parseBoolean(args.argv(1)));

        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_set_linkGroup_onlineAllowed = "<link group> <true|false>";

    public String ac_psu_set_linkGroup_onlineAllowed_$_2(Args args) {

        _psuWriteLock.lock();

        try {
            String linkGroupName = args.argv(0);

            LinkGroup linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                        + linkGroupName);
            }

            linkGroup.setOnlineAllowed(Boolean.parseBoolean(args.argv(1)));

        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_set_linkGroup_nearlineAllowed = "<link group> <true|false>";

    public String ac_psu_set_linkGroup_nearlineAllowed_$_2(Args args) {

        _psuWriteLock.lock();

        try {
            String linkGroupName = args.argv(0);

            LinkGroup linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                        + linkGroupName);
            }

            linkGroup.setNearlineAllowed(Boolean.parseBoolean(args.argv(1)));

        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_clear_im_really_sure = "# don't use this command";

    public String ac_psu_clear_im_really_sure(Args args) {
        // no lock required, while method does it
        clear();
        return "Voila, now everthing is really gone";
    }

    @Override
    public Map<String, SelectionPool> getPools()
    {
        _psuReadLock.lock();
        try {
            return new HashMap<String, SelectionPool>(_pools);
        } finally {
            _psuReadLock.unlock();
        }
    }

    @Override
    public Map<String, SelectionLinkGroup> getLinkGroups()
    {
        _psuReadLock.lock();
        try {
            return new HashMap<String, SelectionLinkGroup>(_linkGroups);
        } finally {
            _psuReadLock.unlock();
        }
    }

    @Override
    public LinkGroup getLinkGroupByName(String linkGroupName) {
        LinkGroup linkGroup = null;
        _psuReadLock.lock();
        try {
            linkGroup = _linkGroups.get(linkGroupName);
        } finally {
            _psuReadLock.unlock();
        }
        return linkGroup;
    }

    @Override
    public Collection<SelectionPoolGroup> getPoolGroupsOfPool(String PoolName) {
        _psuReadLock.lock();
        try {
            Pool pool = _pools.get(PoolName);
            if (pool != null) {
                return new ArrayList<SelectionPoolGroup>(pool._pGroupList.values());
            } else {
                throw new NoSuchElementException(PoolName);
            }
        } finally {
            _psuReadLock.unlock();

        }
    }

    /**
     * Returns true if and only if the pool can stage the given file. That is
     * the only case if the file is located on an HSM connected to the pool.
     */
    private boolean poolCanStageFile(Pool pool, StorageInfo file) {
        boolean rc  = false;
        if (file.locations().isEmpty()
                && pool.getHsmInstances().contains(file.getHsm())) {
            // This is for backwards compatibility until all info
            // extractors support URIs.
            rc = true;
        } else {
            for (URI uri : file.locations()) {
                if (pool.getHsmInstances().contains(uri.getAuthority())) {
                    rc = true;
                }
            }
        }
        _log.debug("{}: matching hsm ({}) found?: {}", pool.getName(), file.getHsm(), rc);
        return rc;
    }

    @Override
    public Collection<SelectionPool> getPoolsByPoolGroup(String poolGroup)
            throws NoSuchElementException {
        PGroup group = _pGroups.get(poolGroup);
        if (group == null) {
            throw new NoSuchElementException("No such pool group: " + poolGroup);
        }
        return new ArrayList<SelectionPool>(group._poolList.values());
    }

    @Override
    public Collection<SelectionPool> getAllDefinedPools(boolean enabledOnly) {
        List<SelectionPool> pools = new ArrayList<>(_pools.size());
        _psuReadLock.lock();
        try {
            for (Pool pool : _pools.values()) {
                if ((!enabledOnly) || pool.isEnabled()) {
                    pools.add(pool);
                }
            }
        } finally {
            _psuReadLock.unlock();
        }
        return pools;
    }

    /** Returns a live view of pools whos name match the given pattern. */
    private Collection<Pool> getPools(Pattern pattern)
    {
        return Maps.filterKeys(_pools, Predicates.contains(pattern)).values();
    }

    /** Returns a displayable string describing a quantity of pools. */
    private String poolCountDescriptionFor(int count)
    {
        switch (count) {
        case 0:
            return "No pools";
        case 1:
            return "One pool";
        default:
            return String.valueOf(count) + " pools";
        }
    }
}
