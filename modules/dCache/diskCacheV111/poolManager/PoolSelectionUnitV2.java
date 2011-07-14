package diskCacheV111.poolManager;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Serializable;

import com.google.common.collect.Maps;
import com.google.common.base.Predicates;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.util.Args;
import dmg.util.CommandSyntaxException;

import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.GenericStorageInfo;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellSetupProvider;
import org.dcache.util.Glob;

public class PoolSelectionUnitV2
    implements Serializable,
               PoolSelectionUnit,
               CellCommandListener,
               CellSetupProvider
{
    private static final String __version = "$Id: PoolSelectionUnitV2.java,v 1.42 2007-10-25 14:03:54 tigran Exp $";
    private static final Logger _log = LoggerFactory.getLogger(PoolSelectionUnitV2.class);


    public String getVersion() {
        return __version;
    }

    private static final int STORE = 1;
    private static final int DCACHE = 2;
    private static final int NET = 3;
    private static final int PROTOCOL = 4;

    static final long serialVersionUID = 4852540784324544199L;

    private final Map<String, PGroup> _pGroups = new HashMap<String, PGroup>();
    private final Map<String, Pool> _pools = new HashMap<String, Pool>();
    private final Map<String, Link> _links = new HashMap<String, Link>();
    private final Map<String, LinkGroup> _linkGroups = new HashMap<String, LinkGroup>();
    private final Map<String, UGroup> _uGroups = new HashMap<String, UGroup>();
    private final Map<String, Unit> _units = new HashMap<String, Unit>();
    private boolean _useRegex = false;
    private boolean _allPoolsActive = false;

    /**
     * Ok, this is the critical part of PoolManager, but (!!!) the whole select
     * path is READ-ONLY, unless we change setup. So ReadWriteLock is what we
     * are looking for, while is a point of serialization.
     */

    private final ReadWriteLock _psuReadWriteLock = new ReentrantReadWriteLock();
    private final Lock _psuReadLock = _psuReadWriteLock.readLock();
    private final Lock _psuWriteLock = _psuReadWriteLock.writeLock();

    private final NetHandler _netHandler = new NetHandler();

    private class NetHandler implements Serializable
    {
        static final long serialVersionUID = 8911153851483100573L;

        private Map<Long, NetUnit>[] _netList = new HashMap[33];
        private String[] _maskStrings = new String[33];
        private long[] _masks = new long[33];

        private NetHandler() {
            long mask = 0;
            long xmask = 0;
            long cursor = 1;
            for (int i = 0; i < _maskStrings.length; i++) {

                _masks[i] = xmask = ~mask;

                int a = (int) ((xmask >> 24) & 0xff);
                int b = (int) ((xmask >> 16) & 0xff);
                int c = (int) ((xmask >> 8) & 0xff);
                int d = (int) ((xmask) & 0xff);

                _maskStrings[i] = a + "." + b + "." + c + "." + d;

                mask |= cursor;
                cursor <<= 1;
            }
        }

        private void clear() {
            for (int i = 0; i < _netList.length; i++)
                if (_netList[i] != null)
                    _netList[i].clear();
        }

        private long inetAddressToLong(InetAddress address) {
            byte[] raw = address.getAddress();
            long addr = 0L;

            for (int i = 0; i < raw.length; i++) {
                addr <<= 8;
                addr |=  raw[i] & 0xff;
            }
            return addr;
        }

        private String longAddressToString(long addr) {
            int a = (int) ((addr >> 24) & 0xff);
            int b = (int) ((addr >> 16) & 0xff);
            int c = (int) ((addr >> 8) & 0xff);
            int d = (int) ((addr) & 0xff);

            return a + "." + b + "." + c + "." + d;
        }

        private void add(NetUnit net) {
            int bit = net.getHostBits();
            if (_netList[bit] == null)
                _netList[bit] = new HashMap<Long, NetUnit>();

            long addr = inetAddressToLong(net.getHostAddress());
            _netList[bit].put(Long.valueOf(addr & _masks[bit]), net);
        }

        private void remove(NetUnit net) {

            int bit = net.getHostBits();
            if (_netList[bit] == null)
                return;

            long addr = inetAddressToLong(net.getHostAddress());

            _netList[bit].remove(Long.valueOf(addr));
            if (_netList.length == 0)
                _netList[bit] = null;
        }

        private NetUnit find(NetUnit net) {

            int bit = net.getHostBits();
            if (_netList[bit] == null)
                return null;

            long addr = inetAddressToLong(net.getHostAddress());

            return _netList[bit].get(Long.valueOf(addr & _masks[bit]));
        }

        private NetUnit match(String inetAddress) throws UnknownHostException {
            long addr = inetAddressToLong(InetAddress.getByName(inetAddress));
            long mask = 0;
            long cursor = 1;
            NetUnit unit = null;
            for (Map map : _netList) {
                if (map != null) {
                    Long l = Long.valueOf(addr & ~mask);
                    unit = (NetUnit) map.get(l);
                    if (unit != null)
                        return unit;
                }
                mask |= cursor;
                cursor <<= 1;
            }
            return null;
        }

        private long bitsToMask(int bits) {
            return _masks[bits];
        }

        private String bitsToString(int bits) {
            return _maskStrings[bits];
        }
    }

    protected static class PoolCore implements Serializable
    {
        static final long serialVersionUID = -8571296485927073985L;

        protected final String _name;
        protected final Map<String, Link> _linkList = new HashMap<String, Link>();

        protected PoolCore(String name) {
            _name = name;
        }

        public String getName() {
            return _name;
        }
    }

    private static class PGroup extends PoolCore
    {
        static final long serialVersionUID = 3883973457610397314L;

        private final Map<String, Pool> _poolList = new HashMap<String, Pool>();

        private PGroup(String name) {
            super(name);
        }

        @Override
        public String toString() {
            return _name + "  (links=" + _linkList.size() + ";pools="
                    + _poolList.size() + ")";
        }
    }

    private static class LinkGroup
        implements SelectionLinkGroup, Serializable
    {
        static final long serialVersionUID = 5425784079451748166L;

        private final String _name;
        private final Collection<SelectionLink> _links = new HashSet<SelectionLink>();
        // no duplicates is allowed
        private final Map<String, Set<String>> _attributes = new HashMap<String, Set<String>>();

        /*
         * my personal view to default behavior
         */
        private boolean _isNearlineAllowed = true;
        private boolean _isOnlineAllowed = false;
        private boolean _isOutputAllowed = true;
        private boolean _isReplicaAllowed = true;
        private boolean _isCustodialAllowed = true;

        LinkGroup(String name) {
            _name = name;
        }

        public String getName() {
            return _name;
        }

        public void add(SelectionLink link) {
            _links.add(link);
        }

        public boolean remove(SelectionLink link) {
            return _links.remove(link);
        }

        public Collection<SelectionLink> links() {
            return _links;
        }

        public void attribute(String attribute, String value, boolean replace) {

            Set<String> valuesSet = null;
            if (!_attributes.containsKey(attribute)) {
                valuesSet = new HashSet<String>();
                _attributes.put(attribute, valuesSet);
            } else {
                valuesSet = _attributes.get(attribute);
                if (replace) {
                    valuesSet.clear();
                }
            }

            valuesSet.add(value);

        }

        public Set<String> attribute(String attribute) {
            return _attributes.get(attribute);
        }

        /**
         *
         * remove a value associated with a attribute if attribute is empty,
         * remove attribute as well.
         *
         * @param attribute
         * @param value
         */
        public void removeAttribute(String attribute, String value) {

            if (_attributes.containsKey(attribute)) {

                Set<String> valuesSet = _attributes.get(attribute);
                valuesSet.remove(value);
                if (valuesSet.isEmpty()) {
                    _attributes.remove(attribute);
                }
            }
        }

        public Map<String, Set<String>> attributes() {
            return new HashMap<String, Set<String>>(_attributes);
        }

        @Override
        public String toString() {

            StringBuilder sb = new StringBuilder(_name);
            sb.append(" : ");

            if (!_links.isEmpty()) {
                sb.append("[  ");
                for (SelectionLink link : _links) {
                    sb.append(link.getName());
                    sb.append(" ");
                }
                sb.append("]");
            } else {
                sb.append("[EMPTY]");
            }

            sb.append("\n");
            sb.append("    Attributes:\n");
            for (Map.Entry<String, Set<String>> aAttribute : _attributes
                    .entrySet()) {
                sb.append("           ").append(aAttribute.getKey()).append(
                        " = ");
                for (String aAttributeValue : aAttribute.getValue()) {
                    sb.append(aAttributeValue).append(" ");
                }
                sb.append("\n");
            }
            sb.append("    AccessLatency:\n");
            sb.append("           ").append("onlineAllowed=").append(_isOnlineAllowed).append("\n");
            sb.append("           ").append("nearlineAllowed=").append(_isNearlineAllowed).append("\n");
            sb.append("    RetentionPolicy:\n");
            sb.append("           ").append("custodialAllowed=").append(_isCustodialAllowed).append("\n");
            sb.append("           ").append("outputAllowed=").append(_isOutputAllowed).append("\n");
            sb.append("           ").append("replicaAllowed=").append(_isReplicaAllowed).append("\n");
            return sb.toString();
        }

        public boolean contains(SelectionLink link) {
            return _links.contains(link);
        }

        public Collection<SelectionLink> getAllLinks() {
            return _links;
        }

        public void addTo(LinkGroup newLinks) {
            _links.addAll(newLinks.getAllLinks());
        }

        public boolean isCustodialAllowed() {
            return _isCustodialAllowed;
        }

        public boolean isNearlineAllowed() {
            return _isNearlineAllowed;
        }

        public boolean isOnlineAllowed() {
            return _isOnlineAllowed;
        }

        public boolean isOutputAllowed() {
            return _isOutputAllowed;
        }

        public boolean isReplicaAllowed() {
            return _isReplicaAllowed;
        }

        public void setCustodialAllowed(boolean isAllowed) {
            _isCustodialAllowed = isAllowed;
        }

        public void setNearlineAllowed(boolean isAllowed) {
            _isNearlineAllowed = isAllowed;
        }

        public void setOnlineAllowed(boolean isAllowed) {
            _isOnlineAllowed = isAllowed;
        }

        public void setOutputAllowed(boolean isAllowed) {
            _isOutputAllowed = isAllowed;
        }

        public void setReplicaAllowed(boolean isAllowed) {
            _isReplicaAllowed = isAllowed;
        }

    }

    private class Pool extends PoolCore implements SelectionPool
    {
        static final long serialVersionUID = 8108406418388363116L;

        private final Map<String, PGroup> _pGroupList = new HashMap<String, PGroup>();
        private boolean _enabled = true;
        private long _active = 0L;
        private boolean _ping = true;
        private long _serialId = 0L;
        private boolean _rdOnly = false;
        private Set<String> _hsmInstances = new HashSet<String>(0);
        private PoolV2Mode  _mode =
            new PoolV2Mode(PoolV2Mode.ENABLED);

        private Pool(String name) {
            super(name);
        }

        public void setActive(boolean active)
        {
            _active =  active ? System.currentTimeMillis() : 0;
        }

        public long getActive() {
            return _ping ? (System.currentTimeMillis() - _active) : 0L;
        }

        /**
         * Returns true if pool heartbeat was received within the last
         * 5 minutes.
         */
        public boolean isActive()
        {
            return getActive() < 5*60*1000;
        }

        public void setReadOnly(boolean rdOnly) {
            _rdOnly = rdOnly;
        }

        public boolean isReadOnly() {
            return _rdOnly;
        }

        /**
         * Returns true if reading from the pool is allowed.
         */
        public boolean canRead()
        {
            return isEnabled() &&
                _mode.getMode() != PoolV2Mode.DISABLED &&
                !_mode.isDisabled(PoolV2Mode.DISABLED_FETCH) &&
                !_mode.isDisabled(PoolV2Mode.DISABLED_DEAD);
        }

        /**
         * Returns true if writing to the pool is allowed. Since we
         * cannot distinguish between a client write and a
         * pool-to-pool write, both operations must be enabled on the
         * pool.
         */
        public boolean canWrite()
        {
            return isEnabled() && !isReadOnly() &&
                _mode.getMode() != PoolV2Mode.DISABLED &&
                !_mode.isDisabled(PoolV2Mode.DISABLED_STORE) &&
                !_mode.isDisabled(PoolV2Mode.DISABLED_DEAD) &&
                !_mode.isDisabled(PoolV2Mode.DISABLED_P2P_SERVER);
        }

        /**
         * Returns true if the pool is allowed to read from tape.
         */
        public boolean canReadFromTape()
        {
            return isEnabled() && !isReadOnly() &&
                _mode.getMode() != PoolV2Mode.DISABLED &&
                !_mode.isDisabled(PoolV2Mode.DISABLED_STAGE) &&
                !_mode.isDisabled(PoolV2Mode.DISABLED_DEAD);
        }

        /**
         * Returns true if the pool can deliver files for P2P
         * operations.
         */
        public boolean canReadForP2P()
        {
            return isEnabled() &&
                _mode.getMode() != PoolV2Mode.DISABLED &&
                !_mode.isDisabled(PoolV2Mode.DISABLED_P2P_SERVER) &&
                !_mode.isDisabled(PoolV2Mode.DISABLED_DEAD);
        }

        /**
         * Returns true if the pool can receive files for P2P
         * operations.
         */
        public boolean canWriteForP2P()
        {
            return isEnabled() && !isReadOnly() &&
                _mode.getMode() != PoolV2Mode.DISABLED &&
                !_mode.isDisabled(PoolV2Mode.DISABLED_P2P_CLIENT) &&
                !_mode.isDisabled(PoolV2Mode.DISABLED_DEAD);
        }

        public void setEnabled(boolean enabled) {
            _enabled = enabled;
        }

        public boolean isEnabled() {
            return _enabled;
        }

        public void setPing(boolean ping) {
            _ping = ping;
        }

        public boolean isPing() {
            return _ping;
        }

        @Override
        public String toString() {
            return _name
                + "  (enabled=" + _enabled
                + ";active=" +(_active > 0 ? (getActive() / 1000) : "no")
                + ";rdOnly=" + isReadOnly()
                + ";links=" + _linkList.size()
                + ";pgroups=" + _pGroupList.size()
                + ";hsm=" + _hsmInstances.toString()
                + ";mode=" + _mode
                + ")";
        }

        public boolean setSerialId(long serialId) {
            if (serialId == _serialId)
                return false;
            _serialId = serialId;
            return true;
        }

        public void setPoolMode(PoolV2Mode mode)
        {
            _mode = mode;
        }

        public PoolV2Mode getPoolMode()
        {
            return _mode;
        }

        public Set<String> getHsmInstances() {
            return _hsmInstances;
        }

        public void setHsmInstances(Set<String> hsmInstances) {
            if (hsmInstances == null) {
                hsmInstances = new HashSet<String>(0);
            }

            _hsmInstances = hsmInstances;
        }
    }

    private static class Link
        implements SelectionLink, Serializable
    {
        static final long serialVersionUID = 4480385941491281821L;

        private final String _name;
        private final Map<String, PoolCore> _poolList = new HashMap<String, PoolCore>();
        private final Map<String, UGroup> _uGroupList = new HashMap<String, UGroup>();

        private int _readPref = 0;
        private int _writePref = 0;
        private int _cachePref = 0;
        private int _p2pPref = -1;
        private String _tag = null;
        private LinkGroup _linkGroup = null;

        public String getTag() {
            return _tag;
        }

        public String getName() {
            return _name;
        }

        private Link(String name) {
            _name = name;
        }

        @Override
        public String toString() {
            return _name + "  (pref=" + _readPref + "/" + _cachePref + "/"
                    + _p2pPref + "/" + _writePref + ";"
                    + (_tag == null ? "" : _tag) + ";" + "ugroups="
                    + _uGroupList.size() + ";pools=" + _poolList.size() + ")";
        }

        public String getAttraction() {
            return "-readpref=" + _readPref + " -writepref=" + _writePref
                    + " -cachepref=" + _cachePref + " -p2ppref=" + _p2pPref
                    + (_tag == null ? "" : " -section=" + _tag);
        }

        public Collection<SelectionPool> pools() {
            List<SelectionPool> list = new ArrayList<SelectionPool>();

            for (Object o : _poolList.values()) {
                if (o instanceof Pool) {
                    list.add((Pool) o);
                } else if (o instanceof PGroup) {
                    for (Pool pool : ((PGroup) o)._poolList.values()) {
                        list.add(pool);
                    }
                }
            }
            return list;
        }

        public boolean exec(Map variableMap) {
            return true;
        }

        public void setLinkGroup(LinkGroup lg) {
            _linkGroup = lg;
        }

        public LinkGroup getLinkGroup() {
            return _linkGroup;
        }
    }

    private static class UGroup implements Serializable
    {
        static final long serialVersionUID = 8169708306745935858L;

        private final String _name;
        private final Map<String, Link> _linkList = new HashMap<String, Link>();
        private final Map<String, Unit> _unitList = new HashMap<String, Unit>(); // !!!
                                                                                    // DCache,
                                                                                    // STore,
                                                                                    // Net
                                                                                    // names
                                                                                    // must
                                                                                    // be
                                                                                    // different

        private UGroup(String name) {
            _name = name;
        }

        private String getName() {
            return _name;
        }

        @Override
        public String toString() {
            return _name + "  (links=" + _linkList.size() + ";units="
                    + _unitList.size() + ")";
        }
    }

    private static class Unit implements Serializable
    {
        static final long serialVersionUID = -2534629882175347637L;

        private String _name = null;
        private int _type = 0;
        private Map<String, UGroup> _uGroupList = new HashMap<String, UGroup>();

        private Unit(String name, int type) {
            _name = name;
            _type = type;
        }

        public String getName() {
            return _name;
        }

        public String getCanonicalName() {
            return getName();
        }

        protected void setName(String name) {
            _name = name;
        }

        private String getType() {
            return _type == STORE ? "Store" : _type == DCACHE ? "DCache"
                    : _type == PROTOCOL ? "Protocol" : _type == NET ? "Net"
                            : "Unknown";
        }

        @Override
        public String toString() {
            return _name + "  (type=" + getType() + ";canonical="
                    + getCanonicalName() + ";uGroups=" + _uGroupList.size()
                    + ")";
        }
    }

    public SelectionLink getLinkByName(String name) throws NoSuchElementException {

        Link link = null;

        _psuReadLock.lock();
        try {
            link = _links.get(name);
            if (link == null)
                throw new NoSuchElementException("Link not found : " + name);
        } finally {
            _psuReadLock.unlock();
        }
        return link;
    }

    public String[] getDefinedPools(boolean enabledOnly) {

        List<String> list = new ArrayList<String>();
        _psuReadLock.lock();
        try {

            for (Pool pool : _pools.values()) {
                if ((!enabledOnly) || pool.isEnabled())
                    list.add(pool.getName());
            }
        } finally {
            _psuReadLock.unlock();
        }
        return list.toArray(new String[list.size()]);
    }

    public String[] getActivePools() {
        List<String> list = new ArrayList<String>();

        _psuReadLock.lock();
        try {
            for (Pool pool : _pools.values()) {
                if (pool.isEnabled() && pool.isActive())
                    list.add(pool.getName());
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
                int type = unit._type;
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

                if (!pool.isPing())
                    pw.append(" -noping");
                if (!pool.isEnabled())
                    pw.append(" -disabled");

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

                for (Map.Entry<String, Set<String>> aAttribute : linkGroup
                        .attributes().entrySet()) {

                    String attributeName = aAttribute.getKey();
                    for (String aAttributeValue : aAttribute.getValue()) {
                        pw.append("psu set linkGroup attribute ").append(
                                linkGroup.getName()).append(" ").append(
                                attributeName).append("=").println(
                                aAttributeValue);
                    }
                }

                for (SelectionLink link : linkGroup.links()) {
                    pw.append("psu addto linkGroup ").append(
                            linkGroup.getName()).append(" ").println(
                            link.getName());
                }
            }

        } finally {
            _psuReadLock.unlock();
        }
    }

    private class NetUnit extends Unit
    {
        static final long serialVersionUID = -2510355260024374990L;

        private InetAddress _address = null;
        private long _mask = 0;
        private int _bits = 0;
        private String _canonicalName = null;

        private NetUnit(String name) throws UnknownHostException {
            super(name, NET);

            int n = name.indexOf('/');
            if (n < 0) {
                //
                // no netmask found (is -host)
                //
                _address = InetAddress.getByName(name);
                //
            } else {
                if ((n == 0) || (n == (name.length() - 1)))
                    throw new IllegalArgumentException(
                            "host or net part missing");

                String hostPart = name.substring(0, n);
                String netPart = name.substring(n + 1);

                //
                // count hostbits
                //
                byte[] raw = InetAddress.getByName(netPart).getAddress();
                _mask = ( (raw[0] & 0xff) << 24)
                        | (( raw[1] & 0xff) << 16)
                        | (( raw[2] & 0xff) << 8)
                        | (raw[3] & 0xff);
                long cursor = 1;
                _bits = 0;
                for (_bits = 0; _bits < 32; _bits++) {
                    if ((_mask & cursor) > 0)
                        break;
                    cursor <<= 1;
                }
                _address = InetAddress.getByName(hostPart);
            }
            _canonicalName = _address.getHostAddress() + "/"
                    + _netHandler.bitsToString(_bits);
        }

        public int getHostBits() {
            return _bits;
        }

        public InetAddress getHostAddress() {
            return _address;
        }

        @Override
        public String getCanonicalName() {
            return _canonicalName;
        }
    }

    private class StoreUnit extends Unit
    {
        static final long serialVersionUID = 5426561463758225952L;

        private StoreUnit(String name) {
            super(name, STORE);
        }
    }

    private class ProtocolUnit extends Unit
    {
        static final long serialVersionUID = 4588437437939085320L;

        private String _protocol = null;
        private int _version = -1;

        private ProtocolUnit(String name) {
            super(name, PROTOCOL);
            int pos = name.indexOf("/");
            if ((pos < 0) || (pos == 0) || ((name.length() - 1) == pos))
                throw new IllegalArgumentException(
                        "Wrong format for protocol unit <protocol>/<version>");

            _protocol = name.substring(0, pos);
            String version = name.substring(pos + 1);
            try {
                _version = version.equals("*") ? -1 : Integer.parseInt(version);
            } catch (Exception ee) {
                throw new IllegalArgumentException(
                        "Wrong format : Protocol version must be * or numerical");
            }
        }

        @Override
        public String getName() {
            return _protocol + (_version > -1 ? ("/" + _version) : "/*");
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
            if (pool != null)
                pool.setActive(active);
        } finally {
            _psuWriteLock.unlock();
        }
        return;
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

    public SelectionPool getPool(String poolName, boolean create) {
        Pool pool = _pools.get(poolName);
        if ((pool != null) || !create)
            return pool;

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
        if (map == null)
            return newmap;

        Map<String, Link> resultMap = new HashMap<String, Link>();
        for (Link link : map.values()) {
            if (newmap.get(link.getName()) != null)
                resultMap.put(link.getName(), link);
        }
        return resultMap;
    }

    private static class LinkMap {
        private class LinkMapEntry {
            private Link _link;
            private int _counter = 0;

            private LinkMapEntry(Link link) {
                _link = link;
                _counter = link._uGroupList.size() - 1;
            }

            private void touch() {
                _counter--;
            }

            private boolean isTriggered() {
                return _counter < 1;
            }
        }

        private Map<String, LinkMapEntry> _linkHash = new HashMap<String, LinkMapEntry>();

        private Iterator<Link> iterator() {
            List<Link> list = new ArrayList<Link>();
            for (LinkMapEntry e : _linkHash.values()) {
                if (e._counter <= 0)
                    list.add(e._link);
            }
            return list.iterator();
        }

        private void addLink(Link link) {
            LinkMapEntry found = _linkHash.get(link.getName());
            if (found == null) {
                _linkHash.put(link.getName(), new LinkMapEntry(link));
            } else {
                found._counter--;
            }
        }
    }

    private LinkMap match(LinkMap linkMap, Unit unit, LinkGroup linkGroup,
            DirectionType ioType) {
        Map<String, Link> map = match(unit, linkGroup, ioType);
        for (Link link : map.values())
            linkMap.addLink(link);
        return linkMap;
    }

    private static class LinkComparator implements Comparator<Link> {
        private final DirectionType _type;

        private LinkComparator(DirectionType type) {
            _type = type;
        }

        public int compare(Link link1, Link link2) {

            switch (_type) {
                case READ : // read
                    return link1._readPref == link2._readPref ? link1._name
                            .compareTo(link2._name)
                            : link1._readPref > link2._readPref ? -1 : 1;
                case CACHE: // cache
                    return link1._cachePref == link2._cachePref ? link1._name
                            .compareTo(link2._name)
                            : link1._cachePref > link2._cachePref ? -1 : 1;
                case WRITE: // write
                    return link1._writePref == link2._writePref ? link1._name
                            .compareTo(link2._name)
                            : link1._writePref > link2._writePref ? -1 : 1;
                case P2P: // p2p
                    int pref1 = link1._p2pPref < 0 ? link1._readPref
                            : link1._p2pPref;
                    int pref2 = link2._p2pPref < 0 ? link2._readPref
                            : link2._p2pPref;
                    return pref1 == pref2 ? link1._name.compareTo(link2._name)
                            : pref1 > pref2 ? -1 : 1;
            }
            throw new IllegalArgumentException("Wrong comparator mode");
        }
    }

    /**
     * @Guarded by _psuReadLock
     */
    @Override
    public PoolPreferenceLevel[] match(DirectionType type,  String netUnitName, String protocolUnitName,
            StorageInfo storageInfo, String linkGroupName) {

        String storeUnitName = storageInfo.getStorageClass()+"@"+storageInfo.getHsm();
        String dCacheUnitName = storageInfo.getCacheClass();

        _log.debug("running match: type={} store={} dCacheUnit={} net={} protocol={} SI={} linkGoup={}",
                new Object[]{
                    type,
                    storeUnitName,
                    dCacheUnitName,
                    netUnitName,
                    protocolUnitName,
                    storageInfo,
                    linkGroupName
                });

        Map<String, String> variableMap = (storageInfo == null ? null : storageInfo.getMap());

        PoolPreferenceLevel[] result = null;
        _psuReadLock.lock();
        try {
            //
            // resolve the unit from the unitname (or net unit mask)
            //
            // regexp code added by rw2 12/5/02
            // original code is in the else
            //
            List<Unit> list = new ArrayList<Unit>();
            if (storeUnitName != null) {
                if (_useRegex) {
                    Unit universalCoverage = null;
                    Unit classCoverage = null;

                    for (Unit unit : _units.values()) {
                        if (unit._type != STORE)
                            continue;

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
                    if (list.size() == 0) {
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
            // System.out.println("PSUDEBUG : list of units : "+list );
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
            Set<Link> sortedSet = new TreeSet<Link>(new LinkComparator(type));
            // System.out.println("match: number of units to check="+fitCount);

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
                // System.out.println( "PSUDEBUG link : "+link.toString() ) ;
                if ((link._uGroupList.size() <= fitCount)
                        && ((variableMap == null) || link.exec(variableMap))) {

                    sortedSet.add(link);
                    // System.out.println( "PSUDEBUG added : "+link);
                }
            }
            int pref = -1;
            List<List<Link>> listList = new ArrayList<List<Link>>();
            List<Link> current = null;

            switch (type) {

                case READ:
                    for (Link link : sortedSet) {
                        if (link._readPref < 1) {
                            continue;
                        }
                        if (link._readPref != pref) {
                            listList.add(current = new ArrayList<Link>());
                            pref = link._readPref;
                        }
                        current.add(link);
                    }
                    break;
                case CACHE:
                    for (Link link : sortedSet) {
                        if (link._cachePref < 1) {
                            continue;
                        }
                        if (link._cachePref != pref) {
                            listList.add(current = new ArrayList<Link>());
                            pref = link._cachePref;
                        }
                        current.add(link);
                    }
                    break;
                case P2P:
                    for (Link link : sortedSet) {
                        int tmpPref = link._p2pPref < 0 ? link._readPref
                                : link._p2pPref;
                        if (tmpPref < 1) {
                            continue;
                        }
                        if (tmpPref != pref) {
                            listList.add(current = new ArrayList<Link>());
                            pref = tmpPref;
                        }
                        current.add(link);
                    }
                    break;
                case WRITE:
                    for (Link link : sortedSet) {
                        if (link._writePref < 1) {
                            continue;
                        }
                        if (link._writePref != pref) {
                            listList.add(current = new ArrayList<Link>());
                            pref = link._writePref;
                        }
                        current.add(link);
                    }
            }
            // System.out.println("PSUDEBUG : result list : "+listList);
            List<Link>[] x = listList.toArray(new List[listList.size()]);
            result = new PoolPreferenceLevel[x.length];
            //
            // resolve the links to the pools
            //
            for (int i = 0; i < x.length; i++) {

                List<Link> linkList = x[i];
                List<String> resultList = new ArrayList<String>();
                String tag = null;

                for (Link link : linkList) {
                    //
                    // get the link if available
                    //
                    if ((tag == null) && (link._tag != null))
                        tag = link._tag;

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

    public String getProtocolUnit(String protocolUnitName) {
        Unit unit = findProtocolUnit(protocolUnitName);
        return unit == null ? null : unit.getName();
    }

    //
    // Legal formats : <protocol>/<version>
    //
    private boolean _protocolsChecked = false;

    private void protocolConfig() {
        if (_protocolsChecked)
            return;
        _protocolsChecked = true;
        boolean found = false;
        for (Object o : _units.values()) {
            if (o instanceof ProtocolUnit) {
                found = true;
                break;
            }
        }
        if (!found)
            _units.put("*/*", new ProtocolUnit("*/*"));
    }

    public Unit findProtocolUnit(String protocolUnitName) {
        //
        if ((protocolUnitName == null) || (protocolUnitName.length() == 0))
            return null;
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

    public String getNetIdentifier(String address) throws UnknownHostException {

        _psuReadLock.lock();
        try {
            NetUnit unit = _netHandler.match(address);
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

        Map<String, Link> map = new HashMap<String, Link>();

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
            if (mode.equals("on") || mode.equals("true")) {
                _allPoolsActive = true;
            } else if (mode.equals("off") || mode.equals("false")) {
                _allPoolsActive = false;
            } else {
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
        if (unit == null)
            throw new IllegalArgumentException("Host not a unit : "
                    + args.argv(0));
        return unit.toString();
    }

    public final static String hh_psu_match = "[-linkGroup=<link group>] read|cache|write|p2p <store unit>|* <dCache unit>|* <net unit>|* <protocol unit>|* ";

    public String ac_psu_match_$_5(Args args) throws Exception {

        try {
            long start = System.currentTimeMillis();
            StorageInfo si = GenericStorageInfo.valueOf(args.argv(1), args.argv(2));

            PoolPreferenceLevel[] list = match(args.argv(0).equals("*") ? DirectionType.ANY
                    : DirectionType.valueOf(args.argv(0).toUpperCase()),
                    args.argv(3).equals("*") ? null : args.argv(3), args
                            .argv(4).equals("*") ? null : args.argv(4), si,
                    args.getOpt("linkGroup"));
            start = System.currentTimeMillis() - start;

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < list.length; i++) {
                String tag = list[i].getTag();
                sb.append("Preference : ").append(i).append("\n");
                sb.append("       Tag : ").append(tag == null ? "NONE" : tag)
                        .append("\n");
                for (Iterator links = list[i].getPoolList().iterator(); links
                        .hasNext();) {
                    sb.append("  ").append(links.next().toString())
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

    public String ac_psu_match2_$_1_99(Args args) throws Exception {
        StringBuffer sb = new StringBuffer();
        Map<String, Link> map = null;
        int required = args.argc();

        _psuReadLock.lock();
        try {
            for (int i = 0; i < args.argc(); i++) {
                String unitName = args.argv(i);
                Unit unit = _units.get(unitName);
                if (unit == null)
                    throw new IllegalArgumentException("Unit not found : "
                            + unitName);
                // TODO:
                map = match(map, unit, DirectionType.READ);
            }
            String netUnitName = args.getOpt("net");
            if (netUnitName != null) {
                Unit unit = _netHandler.find(new NetUnit(netUnitName));
                if (unit == null)
                    throw new IllegalArgumentException(
                            "Unit not found in netList : " + netUnitName);
                // TODO:
                map = match(map, unit, DirectionType.READ);
            }
            for (Link link : map.values()) {
                if (link._uGroupList.size() != required)
                    continue;
                sb.append("Link : ").append(link.toString()).append("\n");

                for(SelectionPool pool: link.pools()) {
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
            if (_pGroups.get(name) != null)
                throw new IllegalArgumentException("Duplicated entry : " + name);

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
        if (onOff.equals("on")) {
            _useRegex = true;
            retVal = "regex turned on";
        } else if (onOff.equals("off")) {
            _useRegex = false;
            retVal = "regex turned off";
        } else {
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
            if (_pools.get(name) != null)
                throw new IllegalArgumentException("Duplicated entry : " + name);

            Pool pool = new Pool(name);
            if (args.getOpt("noping") != null)
                pool.setPing(false);
            if (args.getOpt("disabled") != null)
                pool.setEnabled(false);
            _pools.put(pool.getName(), pool);
        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_set_pool =
        "<pool glob> enabled|disabled|ping|noping|rdonly|notrdonly";
    public String ac_psu_set_pool_$_2(Args args) throws Exception {
        Pattern pattern = Glob.parseGlobToPattern(args.argv(0));
        String mode = args.argv(1);

        _psuWriteLock.lock();
        try {
            int count = 0;
            for (Pool pool: getPools(pattern)) {
                count++;
                if (mode.equals("enabled")) {
                    pool.setEnabled(true);
                } else if (mode.equals("disabled")) {
                    pool.setEnabled(false);
                } else if (mode.equals("ping")) {
                    pool.setPing(true);
                } else if (mode.equals("noping")) {
                    pool.setPing(false);
                } else if (mode.equals("rdonly")) {
                    pool.setReadOnly(true);
                } else if (mode.equals("notrdonly")) {
                    pool.setReadOnly(false);
                } else {
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

            if (_links.get(name) != null)
                throw new IllegalArgumentException("Duplicated entry : " + name);

            Link link = new Link(name);
            //
            // we have to check if all the ugroups really exists.
            // only after we know, that all exist we can
            // add ourselfs to the uGroupLinkList
            //
            for (int i = 1; i < args.argc(); i++) {
                String uGroupName = args.argv(i);

                UGroup uGroup = _uGroups.get(uGroupName);
                if (uGroup == null)
                    throw new IllegalArgumentException("uGroup not found : "
                            + uGroupName);

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
            if (_uGroups.get(name) != null)
                throw new IllegalArgumentException("Duplicated entry : " + name);

            UGroup group = new UGroup(name);

            _uGroups.put(group.getName(), group);
        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_create_unit = "<unit> -net|-store|-dcache";

    public String ac_psu_create_unit_$_1(Args args) throws UnknownHostException {
        String name = args.argv(0);
        Unit unit = null;
        _psuWriteLock.lock();
        try {
            if (args.getOpt("net") != null) {
                NetUnit net = new NetUnit(name);
                _netHandler.add(net);
                unit = net;
            } else if (args.getOpt("store") != null) {
                unit = new Unit(name, STORE);
            } else if (args.getOpt("dcache") != null) {
                unit = new Unit(name, DCACHE);
            } else if (args.getOpt("protocol") != null) {
                unit = new ProtocolUnit(name);
            }
            if (unit == null)
                throw new IllegalArgumentException(
                        "Unit type missing net/store/dcache/protocol");

            String canonicalName = name; // will use the input name
            if (_units.get(canonicalName) != null)
                throw new IllegalArgumentException("Duplicated entry : "
                        + canonicalName);

            _units.put(canonicalName, unit);
        } finally {
            _psuWriteLock.unlock();
        }
        return "";

    }

    public final static String hh_psu_create_linkGroup = "<group name> [-reset]";

    public String ac_psu_create_linkGroup_$_1(Args args) {

        String newGroupName = args.argv(0);
        boolean reset = (args.getOpt("reset") != null);

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

    public Object ac_psux_ls_pool_$_0_1(Args args) throws Exception {

        Object xlsResult = null;
        _psuReadLock.lock();
        try {
            if (args.argc() == 0) {
                xlsResult = _pools.keySet().toArray();
            } else {
                String poolName = args.argv(0);
                Pool pool = _pools.get(poolName);
                if (pool == null)
                    throw new IllegalArgumentException("Not found : "
                            + poolName);

                Object[] result = new Object[6];
                result[0] = poolName;
                result[1] = pool._pGroupList.keySet().toArray();
                result[2] = pool._linkList.keySet().toArray();
                result[3] = Boolean.valueOf(pool._enabled);
                result[4] = Long.valueOf(pool.getActive());
                result[5] = Boolean.valueOf(pool._rdOnly);
                xlsResult = result;
            }
        } finally {
            _psuReadLock.unlock();
        }

        return xlsResult;
    }

    public final static String hh_psux_ls_pgroup = "[<pool group>]";

    public Object ac_psux_ls_pgroup_$_0_1(Args args) throws Exception {

        Object xlsResult = null;
        _psuReadLock.lock();
        try {

            if (args.argc() == 0) {
                xlsResult = _pGroups.keySet().toArray();
            } else {

                String groupName = args.argv(0);
                PGroup group = _pGroups.get(groupName);
                if (group == null)
                    throw new IllegalArgumentException("Not found : "
                            + groupName);

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
    public Collection<String> getPoolGroups() {
        _psuReadLock.lock();
        try {
            return new ArrayList(_pGroups.keySet());
        } finally {
            _psuReadLock.unlock();
        }
    }

    public final static String hh_psux_ls_unit = "[<unit>]";

    public Object ac_psux_ls_unit_$_0_1(Args args) throws Exception {

        Object xlsResult = null;
        _psuReadLock.lock();
        try {

            if (args.argc() == 0) {
                xlsResult = _units.keySet().toArray();
            } else {
                String unitName = args.argv(0);
                Unit unit = _units.get(unitName);
                if (unit == null)
                    throw new IllegalArgumentException("Not found : "
                            + unitName);

                Object[] result = new Object[3];
                result[0] = unitName;
                result[1] = unit._type == STORE ? "Store"
                        : unit._type == PROTOCOL ? "Protocol"
                                : unit._type == DCACHE ? "dCache"
                                        : unit._type == NET ? "Net" : "Unknown";
                result[2] = unit._uGroupList.keySet().toArray();
                xlsResult = result;
            }
        } finally {
            _psuReadLock.unlock();
        }

        return xlsResult;
    }

    public final static String hh_psux_ls_ugroup = "[<unit group>]";

    public Object ac_psux_ls_ugroup_$_0_1(Args args) throws Exception {

        Object xlsResult = null;
        _psuReadLock.lock();
        try {
            if (args.argc() == 0) {
                xlsResult = _uGroups.keySet().toArray();
            } else {
                String groupName = args.argv(0);
                UGroup group = _uGroups.get(groupName);
                if (group == null)
                    throw new IllegalArgumentException("Not found : "
                            + groupName);

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

    public Object ac_psux_ls_link_$_0_1(Args args) throws Exception {

        boolean resolve = args.getOpt("resolve") != null;
        Object xlsResult = null;
        _psuReadLock.lock();
        try {

            if (args.argc() == 0) {
                if (args.getOpt("x") == null) {
                    xlsResult = _links.keySet().toArray();
                } else {
                    List array = new ArrayList();
                    for (Link link : _links.values()) {
                        array.add(fillLinkProperties(link, resolve));
                    }
                    xlsResult = array;
                }
            } else {
                String linkName = args.argv(0);
                Link link = _links.get(linkName);
                if (link == null)
                    throw new IllegalArgumentException("Not found : "
                            + linkName);

                xlsResult = fillLinkProperties(link, resolve);
            }
        } finally {
            _psuReadLock.unlock();
        }

        return xlsResult;
    }

    private Object[] fillLinkProperties(Link link) {
        return fillLinkProperties(link, false);
    }

    private Object[] fillLinkProperties(Link link, boolean resolve) {
        List<String> pools = new ArrayList<String>();
        List<String> groups = new ArrayList<String>();
        for (PoolCore core : link._poolList.values()) {
            if (core instanceof Pool) {
                pools.add(core.getName());
            } else {
                groups.add(core.getName());
                if (!resolve)
                    continue;
                PGroup pg = (PGroup) core;
                if (pg._poolList == null)
                    continue;
                for (String poolName : pg._poolList.keySet()) {
                    pools.add(poolName);
                }
            }
        }

        Object[] result = new Object[resolve ? 13 : 9];
        result[0] = link._name;
        result[1] = Integer.valueOf(link._readPref);
        result[2] = Integer.valueOf(link._cachePref);
        result[3] = Integer.valueOf(link._writePref);
        result[4] = link._uGroupList.keySet().toArray();
        result[5] = pools.toArray();
        result[6] = groups.toArray();
        result[7] = Integer.valueOf(link._p2pPref);
        result[8] = link._tag;

        if ((!resolve) || (link._uGroupList == null))
            return result;

        List<String> net = new ArrayList<String>();
        List<String> protocol = new ArrayList<String>();
        List<String> dcache = new ArrayList<String>();
        List<String> store = new ArrayList<String>();

        for (UGroup ug : link._uGroupList.values()) {
            if (ug._unitList == null)
                continue;
            for (Unit unit : ug._unitList.values()) {
                switch (unit._type) {
                    case NET:
                        net.add(unit._name);
                        break;
                    case PROTOCOL:
                        protocol.add(unit._name);
                        break;
                    case DCACHE:
                        dcache.add(unit._name);
                        break;
                    case STORE:
                        store.add(unit._name);
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

    public Object ac_psux_match_$_5(Args args) throws Exception {

        StorageInfo si = GenericStorageInfo.valueOf(args.argv(1), args.argv(2));

        PoolPreferenceLevel[] list = match(DirectionType.valueOf(args.argv(0).toUpperCase()),
                args.argv(3).equals("*") ? null : args.argv(3),
                args.argv(4).equals("*") ? null : args.argv(4), si, args.getOpt("linkGroup"));
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
        StringBuffer sb = new StringBuffer();
        boolean more = args.getOpt("a") != null;
        boolean detail = (args.getOpt("l") != null) || more;

        _psuReadLock.lock();
        try {
            Collection<Pool> pools;
            if (args.argc() == 0) {
                pools = _pools.values();
            } else {
                pools = new ArrayList<Pool>();
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
        StringBuffer sb = new StringBuffer();
        boolean more = args.getOpt("a") != null;
        boolean detail = (args.getOpt("l") != null) || more;

        _psuReadLock.lock();
        try {
            Iterator i = null;
            if (args.argc() == 0) {
                i = _pGroups.values().iterator();
            } else {
                ArrayList l = new ArrayList();
                for (int n = 0; n < args.argc(); n++) {
                    Object o = _pGroups.get(args.argv(n));
                    if (o != null)
                        l.add(o);
                }
                i = l.iterator();
            }
            while (i.hasNext()) {
                PGroup group = (PGroup) i.next();
                sb.append(group.getName()).append("\n");
                if (detail) {
                    sb.append(" linkList :\n");
                    Iterator i2 = group._linkList.values().iterator();
                    while (i2.hasNext()) {
                        sb.append("   ").append(i2.next().toString()).append(
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

        StringBuffer sb = new StringBuffer();
        boolean more = args.getOpt("a") != null;
        boolean detail = (args.getOpt("l") != null) || more;

        _psuReadLock.lock();
        try {
            Iterator i = null;
            if (args.argc() == 0) {
                i = _links.values().iterator();
            } else {
                ArrayList l = new ArrayList();
                for (int n = 0; n < args.argc(); n++) {
                    Object o = _links.get(args.argv(n));
                    if (o != null)
                        l.add(o);
                }
                i = l.iterator();
            }
            while (i.hasNext()) {
                Link link = (Link) i.next();
                sb.append(link.getName()).append("\n");
                if (detail) {
                    sb.append(" readPref  : ").append(link._readPref).append(
                            "\n");
                    sb.append(" cachePref : ").append(link._cachePref).append(
                            "\n");
                    sb.append(" writePref : ").append(link._writePref).append(
                            "\n");
                    sb.append(" p2pPref   : ").append(link._p2pPref).append(
                            "\n");
                    sb.append(" section   : ").append(
                            link._tag == null ? "None" : link._tag)
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

        StringBuffer sb = new StringBuffer();
        boolean more = args.getOpt("a") != null;
        boolean detail = (args.getOpt("l") != null) || more;

        _psuReadLock.lock();
        try {
            Iterator i = null;
            if (args.argc() == 0) {
                i = _uGroups.values().iterator();
            } else {
                ArrayList l = new ArrayList();
                for (int n = 0; n < args.argc(); n++) {
                    Object o = _uGroups.get(args.argv(n));
                    if (o != null)
                        l.add(o);
                }
                i = l.iterator();
            }
            while (i.hasNext()) {
                UGroup group = (UGroup) i.next();
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
        StringBuffer sb = new StringBuffer();

        _psuReadLock.lock();
        try {
            for (int i = 0; i < _netHandler._netList.length; i++) {
                Map<Long, NetUnit> map = _netHandler._netList[i];
                if (map == null)
                    continue;
                String stringMask = _netHandler.bitsToString(i);
                sb.append(stringMask).append("/").append(i).append("\n");
                for (NetUnit net : map.values()) {
                    sb.append("   ").append(net.getHostAddress().getHostName());
                    if (i > 0)
                        sb.append("/").append(stringMask);
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
        StringBuffer sb = new StringBuffer();
        boolean more = args.getOpt("a") != null;
        boolean detail = (args.getOpt("l") != null) || more;

        _psuReadLock.lock();
        try {
            Iterator i = null;
            if (args.argc() == 0) {
                i = _units.values().iterator();
            } else {
                ArrayList l = new ArrayList();
                for (int n = 0; n < args.argc(); n++) {
                    Object o = _units.get(args.argv(n));
                    if (o != null)
                        l.add(o);
                }
                i = l.iterator();
            }
            while (i.hasNext()) {
                Unit unit = (Unit) i.next();
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

        boolean isLongOutput = args.getOpt("l") != null;
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

    public String ac_psu_remove_unit_$_1(Args args) throws UnknownHostException {
        String unitName = args.argv(0);

        _psuWriteLock.lock();
        try {
            if (args.getOpt("net") != null) {
                NetUnit netUnit = _netHandler.find(new NetUnit(unitName));
                if (netUnit == null)
                    throw new IllegalArgumentException(
                            "Not found in netList : " + unitName);
                unitName = netUnit.getName();
            }
            Unit unit = _units.get(unitName);
            if (unit == null)
                throw new IllegalArgumentException("Unit not found : "
                        + unitName);

            if (unit instanceof NetUnit)
                _netHandler.remove((NetUnit) unit);

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
            if (group == null)
                throw new IllegalArgumentException("UGroup not found : "
                        + groupName);

            if (group._unitList.size() > 0)
                throw new IllegalArgumentException("UGroup not empty : "
                        + groupName);

            if (group._linkList.size() > 0)
                throw new IllegalArgumentException(
                        "Still link(s) pointing to us : " + groupName);

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
            if (group == null)
                throw new IllegalArgumentException("PGroup not found : " + name);

            //
            // check if empty
            //
            if (group._poolList.size() != 0)
                throw new IllegalArgumentException("PGroup not empty : " + name);
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
            if (pool == null)
                throw new IllegalArgumentException("Pool not found : " + name);
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

    public String ac_psu_removefrom_ugroup_$_2(Args args) throws UnknownHostException {
        String groupName = args.argv(0);
        String unitName = args.argv(1);

        _psuWriteLock.lock();
        try {
            UGroup group = _uGroups.get(groupName);
            if (group == null)
                throw new IllegalArgumentException("UGroup not found : "
                        + groupName);

            if (args.getOpt("net") != null) {
                NetUnit netUnit = _netHandler.find(new NetUnit(unitName));
                if (netUnit == null)
                    throw new IllegalArgumentException(
                            "Not found in netList : " + unitName);
                unitName = netUnit.getName();
            }
            Unit unit = _units.get(unitName);
            if (unit == null)
                throw new IllegalArgumentException("Unit not found : "
                        + unitName);
            String canonicalName = unit.getCanonicalName();
            if (group._unitList.get(canonicalName) == null)
                throw new IllegalArgumentException(unitName + " not member of "
                        + groupName);

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
            if (pool == null)
                throw new IllegalArgumentException("Pool not found : "
                        + poolName);

            PGroup group = _pGroups.get(groupName);
            if (group == null)
                throw new IllegalArgumentException("PGroup not found : "
                        + groupName);

            if (group._poolList.get(poolName) == null)
                throw new IllegalArgumentException(poolName + " not member of "
                        + groupName);

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
            if (link == null)
                throw new IllegalArgumentException("Link not found : " + name);
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
            if (group == null)
                throw new IllegalArgumentException("Not found : " + pGroupName);
            Pool pool = _pools.get(poolName);
            if (pool == null)
                throw new IllegalArgumentException("Not found : " + poolName);
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
        return;
    }

    public final static String hh_psu_addto_ugroup = "<unit group> <unit>";

    public String ac_psu_addto_ugroup_$_2(Args args) throws UnknownHostException {

        String uGroupName = args.argv(0);
        String unitName = args.argv(1);

        _psuWriteLock.lock();
        try {
            if (args.getOpt("net") != null) {
                NetUnit netUnit = _netHandler.find(new NetUnit(unitName));
                if (netUnit == null)
                    throw new IllegalArgumentException(
                            "Not found in netList : " + unitName);
                unitName = netUnit.getName();
            }
            UGroup group = _uGroups.get(uGroupName);
            if (group == null)
                throw new IllegalArgumentException("Not found : " + uGroupName);
            Unit unit = _units.get(unitName);
            if (unit == null)
                throw new IllegalArgumentException("Not found : " + unitName);

            String canonicalName = unit.getCanonicalName();
            if (group._unitList.get(canonicalName) != null)
                throw new IllegalArgumentException(unitName
                        + " already member of " + uGroupName);

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
            if (link == null)
                throw new IllegalArgumentException("Not found : " + linkName);

            PoolCore core = _pools.get(poolName);
            if (core == null)
                core = _pGroups.get(poolName);
            if (core == null)
                throw new IllegalArgumentException("Not found : " + poolName);

            if (core._linkList.get(linkName) == null)
                throw new IllegalArgumentException(poolName + " not member of "
                        + linkName);

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
            if (link == null)
                throw new IllegalArgumentException("Not found : " + linkName);

            PoolCore core = _pools.get(poolName);
            if (core == null)
                core = _pGroups.get(poolName);
            if (core == null)
                throw new IllegalArgumentException("Not found : " + poolName);

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
        boolean active = args.getOpt("no") == null;

        _psuWriteLock.lock();

        try {
            if (poolName.equals("*")) {
                for (Pool pool : _pools.values()) {
                    pool.setActive(active);
                }
            } else {
                Pool pool = _pools.get(poolName);
                if (pool == null)
                    throw new IllegalArgumentException("Pool not found : "
                            + poolName);
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
            if (link == null)
                throw new IllegalArgumentException("Not found : " + linkName);

            String tmp = args.getOpt("readpref");
            if (tmp != null)
                link._readPref = Integer.parseInt(tmp);
            tmp = args.getOpt("cachepref");
            if (tmp != null)
                link._cachePref = Integer.parseInt(tmp);
            tmp = args.getOpt("writepref");
            if (tmp != null)
                link._writePref = Integer.parseInt(tmp);
            tmp = args.getOpt("p2ppref");
            if (tmp != null)
                link._p2pPref = Integer.parseInt(tmp);
            tmp = args.getOpt("section");
            if (tmp != null)
                link._tag = tmp.equals("NONE") ? null : tmp;

        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_set_linkGroup_attribute = "<link group> [-r] attribute=value";

    public String ac_psu_set_linkGroup_attribute_$_2(Args args) {

        _psuWriteLock.lock();

        try {
            String linkGroupName = args.argv(0);

            LinkGroup linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                        + linkGroupName);
            }

            String[] attrKeyValue = args.argv(1).split("=");

            if (attrKeyValue.length == 1 || attrKeyValue[1] == null
                    || attrKeyValue[1].length() == 0) {
                return "bad value";
            }

            linkGroup.attribute(attrKeyValue[0], attrKeyValue[1], args
                    .getOpt("r") != null);

        } finally {
            _psuWriteLock.unlock();
        }
        return "";
    }

    public final static String hh_psu_remove_linkGroup_attribute = "<link group> attribute=value";

    public String ac_psu_remove_linkGroup_attribute_$_2(Args args) {

        _psuWriteLock.lock();

        try {
            String linkGroupName = args.argv(0);

            LinkGroup linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                throw new IllegalArgumentException("LinkGroup not found : "
                        + linkGroupName);
            }

            String[] attrKeyValue = args.argv(1).split("=");

            if (attrKeyValue.length == 1 || attrKeyValue[1] == null
                    || attrKeyValue[1].length() == 0) {
                return "bad value";
            }
            // remove
            linkGroup.removeAttribute(attrKeyValue[0], attrKeyValue[1]);

        } finally {
            _psuWriteLock.unlock();
        }
        return "";
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

    public String[] getLinkGroups() {

        String[] linkGroups;
        _psuReadLock.lock();
        try {
            linkGroups = _linkGroups.keySet().toArray(
                    new String[_linkGroups.size()]);
        } finally {
            _psuReadLock.unlock();
        }

        return linkGroups;
    }

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

    public String[] getLinksByGroupName(String linkGroupName) throws NoSuchElementException {

        String[] linkNames = null;

        _psuReadLock.lock();
        try {
            LinkGroup linkGroup = _linkGroups.get(linkGroupName);
            if (linkGroup == null) {
                throw new NoSuchElementException("LinkGroup not found : "
                        + linkGroupName);
            }

            Collection<SelectionLink> links = linkGroup.links();
            int count = links.size();
            linkNames = new String[count];
            int j = 0;
            for (SelectionLink link : links) {
                linkNames[j++] = link.getName();
            }
        } finally {
            _psuReadLock.unlock();
        }

        return linkNames;
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
        _log.debug("{}: matching hsm ({}) found?: {}",
                new Object[]{
                    pool.getName(),
                    file.getHsm(),
                    rc
                });
        return rc;
    }

    public Collection<SelectionPool> getPoolsByPoolGroup(String poolGroup)
        throws NoSuchElementException
    {
        PGroup group = _pGroups.get(poolGroup);
        if (group == null)
            throw new NoSuchElementException("No such pool group: " + poolGroup);

        List<SelectionPool> pools = new ArrayList(group._poolList.size());
        for (Pool pool: group._poolList.values()) {
            pools.add(pool);
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
