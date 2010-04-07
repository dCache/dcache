package org.dcache.services.info.stateInfo;

import java.util.Collections;
import java.util.HashMap;
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
        DCACHE("dcache", "D"), STORE("store", "S"), PROTOCOL("protocol", "P"), NETWORK("net", "N");

        private String _pathElement;
        private String _nasNamePrefix;

        UNIT_TYPE( String pathElement, String nasNamePrefix) {
            _pathElement = pathElement;
            _nasNamePrefix = nasNamePrefix;
        }

        public String getPathElement() {
            return _pathElement;
        }

        public String getNasNamePrefix() {
            return _nasNamePrefix;
        }
    }

    /**
     * The four different types of operation that may be allowed on a link.
     * These are read, write, stage and pool-to-pool destination.
     */
    public static enum OPERATION {
        READ("read", "R"), WRITE("write", "W"), CACHE("cache", "C"), P2P("p2p", "P");

        private String _pathElement;
        private String _nasNamePrefix;

        OPERATION( String pathElement, String nasNamePrefix) {
            _pathElement = pathElement;
            _nasNamePrefix = nasNamePrefix;
        }

        public String getPathElement() {
            return _pathElement;
        }

        public String getNasNamePrefix() {
            return _nasNamePrefix;
        }
    }

    private final String _id;
    private final Set<String> _pools = new HashSet<String>();
    private final Set<String> _poolgroups = new HashSet<String>();
    private final Set<String> _unitgroups = new HashSet<String>();
    private final Map<UNIT_TYPE, Set<String>> _units = Collections.unmodifiableMap( new HashMap<UNIT_TYPE, Set<String>>() {
        private static final long serialVersionUID = -3626724207880413521L;
        {
            put( UNIT_TYPE.DCACHE, new HashSet<String>());
            put( UNIT_TYPE.STORE, new HashSet<String>());
            put( UNIT_TYPE.PROTOCOL, new HashSet<String>());
            put( UNIT_TYPE.NETWORK, new HashSet<String>());
        }
    });

    private final Map<OPERATION, Long> _operationPref = new ConcurrentHashMap<OPERATION, Long>();

    public LinkInfo( String id) {
        _id = id;
    }

    protected void addPool( String poolName) {
        synchronized (_pools) {
            _pools.add( poolName);
        }
    }

    protected void addPoolgroup( String poolgroup) {
        synchronized (_poolgroups) {
            _poolgroups.add( poolgroup);
        }
    }

    protected void addUnitgroup( String unitgroupName) {
        synchronized (_unitgroups) {
            _unitgroups.add( unitgroupName);
        }
    }

    protected void addUnit( UNIT_TYPE type, String unitName) {
        Set<String> units;

        units = _units.get( type);

        synchronized (units) {
            units.add( unitName);
        }
    }

    protected void setOperationPref( OPERATION operation, long pref) {
        _operationPref.put( operation, new Long( pref));
    }

    public String getId() {
        return _id;
    }

    public Set<String> getPools() {
        synchronized (_pools) {
            return new HashSet<String>( _pools);
        }
    }

    public Set<String> getPoolgroups() {
        synchronized (_poolgroups) {
            return new HashSet<String>( _poolgroups);
        }
    }

    public Set<String> getUnitgroups() {
        synchronized (_unitgroups) {
            return new HashSet<String>( _unitgroups);
        }
    }

    public Set<String> getUnits( UNIT_TYPE unitType) {
        synchronized (_units) {
            return new HashSet<String>( _units.get( unitType));
        }
    }

    public long getOperationPref( OPERATION operation) {
        Long preference = _operationPref.get( operation);

        // If not defined, assume not accessible for this operation.
        if( preference == null)
            return 0;

        return preference;
    }

    public boolean isAccessableFor( OPERATION operation) {
        long pref = getOperationPref( operation);

        if( operation == OPERATION.P2P)
            return pref > 0 ||
                   (pref == -1 && getOperationPref( OPERATION.READ) > 0);

        return pref > 0;
    }

    @Override
    public int hashCode() {
        return _pools.hashCode() ^ _poolgroups.hashCode() ^ _units.hashCode() ^
               _operationPref.hashCode();
    }

    @Override
    public boolean equals( Object otherObject) {
        if( !(otherObject instanceof LinkInfo))
            return false;

        if( this == otherObject)
            return true;

        LinkInfo otherLink = (LinkInfo) otherObject;

        if( !_pools.equals( otherLink._pools))
            return false;

        if( !_poolgroups.equals( otherLink._poolgroups))
            return false;

        if( !_units.equals( otherLink._units))
            return false;

        if( !_operationPref.equals( otherLink._operationPref))
            return false;

        return true;
    }

    /**
     * A handy method to emit some debugging information about this LinkInfo.
     */
    public String debugInfo() {
        StringBuffer sb = new StringBuffer();

        sb.append( "=== LinkInfo for link \"" + _id + "\" ===\n");

        if( _pools.size() > 0) {
            sb.append( "  Pools:\n");
            for( String poolName : _pools)
                sb.append( "    " + poolName + "\n");
        }

        if( _poolgroups.size() > 0) {
            sb.append( "  Poolgroups:\n");
            for( String poolgroupName : _poolgroups)
                sb.append( "    " + poolgroupName + "\n");
        }

        boolean haveOperation = false;
        for( OPERATION operation : OPERATION.values()) {
            if( _operationPref.containsKey( operation)) {
                haveOperation = true;
                break;
            }
        }
        if( haveOperation) {
            sb.append( "  Preferences:\n");
            for( OPERATION operation : OPERATION.values())
                if( _operationPref.containsKey( operation))
                    sb.append( "    " + operation + ": " +
                               _operationPref.get( operation) + "\n");
        }

        boolean haveUnits = false;
        for( UNIT_TYPE type : UNIT_TYPE.values()) {
            if( _units.get( type).size() > 0) {
                haveUnits = true;
                break;
            }
        }
        if( haveUnits) {
            sb.append( "  Units:\n");
            for( UNIT_TYPE type : UNIT_TYPE.values()) {
                Set<String> units = _units.get( type);

                if( units.size() > 0) {
                    sb.append( "    " + type + ":\n");

                    for( String unitName : units)
                        sb.append( "        " + unitName + "\n");
                }
            }
        }

        return sb.toString();
    }
}