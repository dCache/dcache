package diskCacheV111.vehicles;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

public class GenericStorageInfo
    implements StorageInfo
{
    private static Logger _logger = LoggerFactory.getLogger( GenericStorageInfo.class);

    static final long serialVersionUID = 2089636591513548893L;

    /*
     * to simulate the 'classic' behavior : new files go to tape and, after
     * flushing, removed by sweeper if space needed.
     */
    private AccessLatency _accessLatency = StorageInfo.DEFAULT_ACCESS_LATENCY;
    private RetentionPolicy _retentionPolicy = StorageInfo.DEFAULT_RETENTION_POLICY;

    private HashMap<String, String> _keyHash = new HashMap<String, String>();
    private ArrayList<URI> _locations = new ArrayList<URI>();
    private boolean _setHsm = false;
    private boolean _setStorageClass = false;
    private boolean _setBitFileId = false;
    private boolean _setRetentionPolicy = false;
    private boolean _setAccessLatency = false;
    private boolean _setLocation = false;

    private boolean _isNew = true;
    private boolean _isStored = false;

    private String _hsm = null;
    private String _cacheClass = null;
    private long _fileSize = 0;
    private String _storageClass = null;

    @Deprecated
    private String _bitfileId = null;

    public GenericStorageInfo() {
    }

    public GenericStorageInfo(String hsm, String storageClass) {

        _storageClass = storageClass;
        _hsm = hsm;
    }

    @Deprecated
    public void addKeys(Map<String, String> keys) {
        _keyHash.putAll(keys);
    }

    public void addLocation(URI newLocation) {
        _locations.add(newLocation);
    }

    public AccessLatency getAccessLatency() {
        return _accessLatency;
    }

    @Deprecated
    public String getBitfileId() {
        return _bitfileId == null ? "<Unknown>" : _bitfileId;
    }

    public String getCacheClass() {
        return _cacheClass;
    }

    public long getFileSize() {
        return _fileSize;
    }

    public String getHsm() {
        return _hsm;
    }

    @Deprecated
    public String getKey(String key) {
        return _keyHash.get(key);
    }

    @Deprecated
    public Map<String, String> getMap() {
        return new HashMap<String, String>(_keyHash);
    }

    public RetentionPolicy getRetentionPolicy() {
        return _retentionPolicy;
    }

    public String getStorageClass() {
        return _storageClass;
    }

    public boolean isCreatedOnly() {
        return _isNew;
    }

    public boolean isSetAccessLatency() {
        return _setAccessLatency;
    }

    public void isSetAccessLatency(boolean isSet) {
        _setAccessLatency = isSet;
    }

    public boolean isSetAddLocation() {
        return _setLocation;
    }

    public void isSetAddLocation(boolean isSet) {
        _setLocation = isSet;
    }

    @Deprecated
    public boolean isSetBitFileId() {
        return _setBitFileId;
    }

    @Deprecated
    public void isSetBitFileId(boolean isSet) {
        _setBitFileId = isSet;
    }

    public void setCacheClass(String newCacheClass) {
        _cacheClass = newCacheClass;
    }

    public boolean isSetHsm() {
        return _setHsm;
    }

    public void isSetHsm(boolean isSet) {
        _setHsm = isSet;
    }

    public boolean isSetRetentionPolicy() {
        return _setRetentionPolicy;
    }

    public void isSetRetentionPolicy(boolean isSet) {
        _setRetentionPolicy = isSet;
    }

    public boolean isSetStorageClass() {
        return _setStorageClass;
    }

    public void isSetStorageClass(boolean isSet) {
        _setStorageClass = isSet;
    }

    /**
     *
     * @return true if locations list is not empty or ( legacy case )
     * if value was explicit set by setIsStored(true)
     */
    public boolean isStored() {
        /*
         * FIXME: _locations!= null is needed to read old SI files
         */
        return _isStored || (_locations != null && !_locations.isEmpty());
    }

    public List<URI> locations() {
        return _locations;
    }

    public void setAccessLatency(AccessLatency accessLatency) {
        _accessLatency = accessLatency;

    }

    @Deprecated
    public void setBitfileId(String bitfileId) {
        _bitfileId = bitfileId;
    }

    public void setFileSize(long fileSize) {
        _fileSize = fileSize;
    }

    public void setHsm(String newHsm) {
        _hsm = newHsm;
    }

    public void setIsNew(boolean isNew) {
        _isNew = isNew;
    }

    @Deprecated
    public void setKey(String key, String value) {
        if (value == null) {
            _keyHash.remove(key);
        } else {
            _keyHash.put(key, value);
        }
    }

    public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
        _retentionPolicy = retentionPolicy;
    }

    public void setStorageClass(String newStorageClass) {
        _storageClass = newStorageClass;
    }

    /**
     * @Deprecated the result will generated depending on content of locations
     */
    @Deprecated
    public void setIsStored( boolean isStored) {
        _isStored = isStored;
    }

    @Override
    public String toString() {
        String sc = getStorageClass();
        String cc = getCacheClass();
        String hsm = getHsm();
        AccessLatency ac = getAccessLatency();
        RetentionPolicy rp = getRetentionPolicy();
        StringBuilder sb = new StringBuilder();
        sb.append("size=").append(getFileSize()).append(";new=").append(
                isCreatedOnly()).append(";stored=").append(isStored()).append(
                ";sClass=").append(sc == null ? "-" : sc).append(";cClass=")
                .append(cc == null ? "-" : cc).append(";hsm=").append(
                        hsm == null ? "-" : hsm).append(";accessLatency=")
                        .append(ac == null ? "-" : ac.toString()).append(
                        ";retentionPolicy=").append(
                                rp == null ? "-" : rp.toString()).append(";");

        /*
         * FIXME: extra checks are needed to read old SI files
         */
        if( _keyHash != null ) {
            for (Map.Entry<String, String> entry : _keyHash.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                sb.append(key).append("=").append(value).append(";");
            }
        }
        if( _locations != null ) {
            for(URI location : _locations ) {
                sb.append(location).append(";");
            }
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        Set<URI> ourLocations = new HashSet<URI>( locations());

        return getAccessLatency().hashCode() ^
        getRetentionPolicy().hashCode() ^
        ourLocations.hashCode() ^
        (int) getFileSize() ^
        getStorageClass().hashCode() ^
        (isStored() ? 1 << 0 : 0) ^
        (isCreatedOnly() ? 1 << 1 : 0);
    }

    @Override
    public boolean equals( Object o) {

        if ( o == this) {
            return true;
        }

        if( !( o instanceof GenericStorageInfo )) {
            return false;
        }

        GenericStorageInfo other = (GenericStorageInfo)o;

        if ( !other.getAccessLatency().equals( this.getAccessLatency()) ) {
            return false;
        }
        if ( !other.getRetentionPolicy().equals( this.getRetentionPolicy()) ) {
            return false;
        }

        /**
         * Any non-zero number of occurrences of a URI are considered equivalent.  A necessary
         * condition for equality is that, for each location URI in this object, the other
         * GenericStorageInfo must have at least one instance of the URI.  Having more than one
         * is OK.
         */
        Set<URI> ourLocations = new HashSet<URI>( locations());
        Set<URI> otherLocations = new HashSet<URI>( other.locations());

        if( ! otherLocations.equals( ourLocations)) {
            return false;
        }

        /**
         *  If two GenericStorageInfo objects have location URIs specified then we ignore any
         *  BitfieldId values.
         */
        if( this.locations().isEmpty()) {
            if ( other.getBitfileId() != null && this.getBitfileId() != null  &&
                    ! other.getBitfileId().equals(this.getBitfileId() )) {
                return false;
            }

            if ( other.getBitfileId() != null && this.getBitfileId() == null  ) {
                return false;
            }
            if ( other.getBitfileId() == null && this.getBitfileId() != null  ) {
                return false;
            }
        }

        if( !other.getHsm().equals(this.getHsm() )) {
            return false;
        }

        if( other.getCacheClass() != null && this.getCacheClass() != null &&
                !other.getCacheClass().equals(this.getCacheClass())) {
            return false;
        }

        if( other.getCacheClass() == null && this.getCacheClass() != null ) {
            return false;
        }
        if( other.getCacheClass() != null && this.getCacheClass() == null ) {
            return false;
        }

        if( !(other._keyHash.equals(this._keyHash))) {
            return false;
        }

        if( other.getFileSize() != this.getFileSize() ) {
            return false;
        }
        if( !other.getStorageClass().equals(this.getStorageClass())) {
            return false;
        }

        if( other.isStored() != this.isStored() ) {
            return false;
        }
        if( other.isCreatedOnly() != this.isCreatedOnly() ) {
            return false;
        }

        return true;
    }


    /**
     * pre 1.8 read problems
     * @return
     * @Since 1.8
     */
    Object readResolve() {

        if(_accessLatency == null ) {
            _accessLatency =  AccessLatency.NEARLINE;
        }

        if(_retentionPolicy == null ) {
            _retentionPolicy = RetentionPolicy.CUSTODIAL;
        }

        if(_locations == null ) {
            _locations = new ArrayList<URI>();
        }

        return this;

    }

    @Override
    public GenericStorageInfo clone()
    {
        try {
            GenericStorageInfo copy = (GenericStorageInfo) super.clone();
            copy._keyHash = (HashMap<String,String>) copy._keyHash.clone();
            copy._locations = (ArrayList<URI>) copy._locations.clone();
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone storage info: " +
                                       e.getMessage());
        }
    }

    /**
     * Create a {@link StorageInfo} corresponding to given store unit and cacheClass
     *
     * @param storeUnit
     * @param cacheClass
     * @return StorageInfo
     * @throws IllegalArgumentException if store unit format do not match to
     * x:y@z or equal to '*'
     */
    public static StorageInfo valueOf(String storeUnit, String cacheClass)
            throws IllegalArgumentException {
        StorageInfo si;

        if (storeUnit.equals("*")) {
            si = new GenericStorageInfo();
        } else {

            String[] unitParts = storeUnit.split("@");
            if (unitParts.length != 2) {
                throw new IllegalArgumentException("Invalid format: expected<x:y@z> got <" + storeUnit + ">");
            }
            si = new GenericStorageInfo(unitParts[1], unitParts[0]);
        }

        if (!cacheClass.equals("*")) {
            si.setCacheClass(cacheClass);
        }
        return si;
    }
}
