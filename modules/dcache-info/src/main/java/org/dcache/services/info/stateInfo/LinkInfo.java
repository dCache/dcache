package org.dcache.services.info.stateInfo;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A simple class to store information about a link.
 */
public class LinkInfo {

    /**
     * The different types of unit that may be used to select a link. These
     * are dcache, store, protocol and network.
     */
    public static enum UNIT_TYPE {
        DCACHE("dcache"), STORE("store"), PROTOCOL("protocol"), NETWORK("net");

        private String _pathElement;

        UNIT_TYPE(String pathElement) {
            _pathElement = pathElement;
        }

        public String getPathElement() {
            return _pathElement;
        }
    }

    /**
     * The four different types of operation that may be allowed on a link.
     * These are read, write, stage and pool-to-pool destination.
     */
    public static enum OPERATION {
        READ("read"), WRITE("write"), CACHE("cache"), P2P("p2p");

        private String _pathElement;

        OPERATION(String pathElement) {
            _pathElement = pathElement;
        }

        public String getPathElement() {
            return _pathElement;
        }
    }

    private final String _id;
    private final Set<String> _pools = new HashSet<>();
    private final Set<String> _poolgroups = new HashSet<>();
    private final Set<String> _unitgroups = new HashSet<>();
    private final Multimap<UNIT_TYPE, String> _units = HashMultimap.create();

    private final Map<OPERATION, Long> _operationPref = new ConcurrentHashMap<>();

    public LinkInfo(String id) {
        _id = id;
    }

    protected void addPool(String poolName) {
        synchronized (_pools) {
            _pools.add(poolName);
        }
    }

    protected void addPoolgroup(String poolgroup) {
        synchronized (_poolgroups) {
            _poolgroups.add(poolgroup);
        }
    }

    protected void addUnitgroup(String unitgroupName) {
        synchronized (_unitgroups) {
            _unitgroups.add(unitgroupName);
        }
    }

    protected void addUnit(UNIT_TYPE type, String unitName) {
        synchronized (_units) {
            _units.put(type, unitName);
        }
    }

    protected void setOperationPref(OPERATION operation, long pref) {
        _operationPref.put(operation, pref);
    }

    public String getId() {
        return _id;
    }

    public Set<String> getPools() {
        synchronized (_pools) {
            return ImmutableSet.copyOf(_pools);
        }
    }

    public Set<String> getPoolgroups() {
        synchronized (_poolgroups) {
            return ImmutableSet.copyOf(_poolgroups);
        }
    }

    public Set<String> getUnitgroups() {
        synchronized (_unitgroups) {
            return ImmutableSet.copyOf(_unitgroups);
        }
    }

    public Set<String> getUnits(UNIT_TYPE unitType) {
        synchronized (_units) {
            return ImmutableSet.copyOf(_units.get(unitType));
        }
    }

    public long getOperationPref(OPERATION operation) {
        Long preference = _operationPref.get(operation);

        // If not defined, assume not accessible for this operation.
        if (preference == null) {
            return 0;
        }

        return preference;
    }

    public boolean isAccessableFor(OPERATION operation) {
        long pref = getOperationPref(operation);

        if (operation == OPERATION.P2P) {
            return pref > 0 ||
                    (pref == -1 && getOperationPref(OPERATION.READ) > 0);
        }

        return pref > 0;
    }

    @Override
    public int hashCode() {
        return _pools.hashCode() ^ _poolgroups.hashCode() ^ _units.hashCode() ^
               _operationPref.hashCode();
    }

    @Override
    public boolean equals(Object otherObject) {
        if (!(otherObject instanceof LinkInfo)) {
            return false;
        }

        if (this == otherObject) {
            return true;
        }

        LinkInfo otherLink = (LinkInfo) otherObject;

        if (!_pools.equals(otherLink._pools)) {
            return false;
        }

        if (!_poolgroups.equals(otherLink._poolgroups)) {
            return false;
        }

        if (!_units.equals(otherLink._units)) {
            return false;
        }

        if (!_operationPref.equals(otherLink._operationPref)) {
            return false;
        }

        return true;
    }

    /**
     * A handy method to emit some debugging information about this LinkInfo.
     */
    public String debugInfo() {
        StringBuilder sb = new StringBuilder();

        sb.append("=== LinkInfo for link \"").append(_id).append("\" ===\n");

        if (_pools.size() > 0) {
            sb.append("  Pools:\n");
            for (String poolName : _pools) {
                sb.append("    ").append(poolName).append("\n");
            }
        }

        if (_poolgroups.size() > 0) {
            sb.append("  Poolgroups:\n");
            for (String poolgroupName : _poolgroups) {
                sb.append("    ").append(poolgroupName).append("\n");
            }
        }

        boolean haveOperation = false;
        for (OPERATION operation : OPERATION.values()) {
            if (_operationPref.containsKey(operation)) {
                haveOperation = true;
                break;
            }
        }
        if (haveOperation) {
            sb.append("  Preferences:\n");
            for (OPERATION operation : OPERATION.values()) {
                if (_operationPref.containsKey(operation)) {
                    sb.append("    ").append(operation).append(": ")
                            .append(_operationPref.get(operation)).append("\n");
                }
            }
        }

        boolean haveUnits = false;
        for (UNIT_TYPE type : UNIT_TYPE.values()) {
            if (_units.containsKey(type)) {
                haveUnits = true;
                break;
            }
        }
        if (haveUnits) {
            sb.append("  Units:\n");
            for (UNIT_TYPE type : UNIT_TYPE.values()) {
                Collection<String> units = _units.get(type);

                if (!units.isEmpty()) {
                    sb.append("    ").append(type).append(":\n");

                    for (String unitName : units) {
                        sb.append("        ").append(unitName).append("\n");
                    }
                }
            }
        }

        return sb.toString();
    }
}
