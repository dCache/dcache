package diskCacheV111.vehicles;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

import static java.util.stream.Collectors.toMap;

public class GenericStorageInfo
    implements StorageInfo
{
    private static final long serialVersionUID = 2089636591513548893L;

    /*
     * to simulate the 'classic' behavior : new files go to tape and, after
     * flushing, removed by sweeper if space needed.
     */
    @Deprecated
    private AccessLatency _accessLatency = StorageInfo.DEFAULT_ACCESS_LATENCY;
    @Deprecated
    private RetentionPolicy _retentionPolicy = StorageInfo.DEFAULT_RETENTION_POLICY;

    private Map<String, String> _keyHash = new HashMap<>();
    private List<URI> _locations = new ArrayList<>();
    private boolean _setHsm;
    private boolean _setStorageClass;
    private boolean _setBitFileId;
    private boolean _setLocation;

    private boolean _isNew = true;
    private boolean _isStored;

    private String _hsm;
    private String _cacheClass;
    @Deprecated
    private long _fileSize;
    private String _storageClass;

    @Deprecated
    private String _bitfileId;

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

    @Override
    public void addLocation(URI newLocation) {
        _locations.add(newLocation);
    }

    @Override
    @Deprecated
    public String getBitfileId() {
        return _bitfileId == null ? "<Unknown>" : _bitfileId;
    }

    @Override
    public String getCacheClass() {
        return _cacheClass;
    }

    @Override
    public String getHsm() {
        return _hsm;
    }

    @Override
    @Deprecated
    public String getKey(String key) {
        return _keyHash.get(key);
    }

    @Override
    @Deprecated
    public Map<String, String> getMap() {
        return new HashMap<>(_keyHash);
    }

    @Override
    public String getStorageClass() {
        return _storageClass;
    }

    @Override
    public boolean isCreatedOnly() {
        return _isNew;
    }

    @Override
    public boolean isSetAddLocation() {
        return _setLocation;
    }

    @Override
    public void isSetAddLocation(boolean isSet) {
        _setLocation = isSet;
    }

    @Override
    @Deprecated
    public boolean isSetBitFileId() {
        return _setBitFileId;
    }

    @Override
    @Deprecated
    public void isSetBitFileId(boolean isSet) {
        _setBitFileId = isSet;
    }

    @Override
    public void setCacheClass(String newCacheClass) {
        _cacheClass = newCacheClass;
    }

    @Override
    public boolean isSetHsm() {
        return _setHsm;
    }

    @Override
    public void isSetHsm(boolean isSet) {
        _setHsm = isSet;
    }

    @Override
    public boolean isSetStorageClass() {
        return _setStorageClass;
    }

    @Override
    public void isSetStorageClass(boolean isSet) {
        _setStorageClass = isSet;
    }

    /**
     *
     * @return true if locations list is not empty or ( legacy case )
     * if value was explicit set by setIsStored(true)
     */
    @Override
    public boolean isStored() {
        /*
         * FIXME: _locations!= null is needed to read old SI files
         */
        return _isStored || (_locations != null && !_locations.isEmpty());
    }

    @Override
    public List<URI> locations() {
        return _locations;
    }

    @Override
    @Deprecated
    public void setBitfileId(String bitfileId) {
        _bitfileId = bitfileId;
    }

    @Override
    public void setHsm(String newHsm) {
        _hsm = newHsm;
    }

    @Override
    public void setIsNew(boolean isNew) {
        _isNew = isNew;
    }

    @Override
    @Deprecated
    public String setKey(String key, String value) {
        if (value == null) {
            return _keyHash.remove(key);
        } else {
            return _keyHash.put(key, value);
        }
    }

    @Override
    public void setStorageClass(String newStorageClass) {
        _storageClass = newStorageClass;
    }

    /**
     * @Deprecated the result will generated depending on content of locations
     */
    @Override
    @Deprecated
    public void setIsStored( boolean isStored) {
        _isStored = isStored;
    }

    @Override
    public String toString() {
        String sc = getStorageClass();
        String cc = getCacheClass();
        String hsm = getHsm();
        AccessLatency ac = getLegacyAccessLatency();
        RetentionPolicy rp = getLegacyRetentionPolicy();
        StringBuilder sb = new StringBuilder();
        sb.append("size=").append(getLegacySize()).append(";new=").append(
                isCreatedOnly()).append(";stored=").append(isStored()).append(
                ";sClass=").append(sc == null ? "-" : sc).append(";cClass=")
                .append(cc == null ? "-" : cc).append(";hsm=").append(
                        hsm == null ? "-" : hsm).append(";accessLatency=")
                        .append(ac == null ? "-" : ac.toString()).append(
                        ";retentionPolicy=").append(
                                rp == null ? "-" : rp.toString()).append(';');

        /*
         * FIXME: extra checks are needed to read old SI files
         */
        if( _keyHash != null ) {
            for (Map.Entry<String, String> entry : _keyHash.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                sb.append(key).append('=').append(value).append(';');
            }
        }
        if( _locations != null ) {
            for(URI location : _locations ) {
                sb.append(location).append(';');
            }
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        Set<URI> ourLocations = new HashSet<>( locations());

        return getLegacyAccessLatency().hashCode() ^
        getLegacyRetentionPolicy().hashCode() ^
        ourLocations.hashCode() ^
        (int) getLegacySize() ^
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

        if ( !other.getLegacyAccessLatency().equals( this.getLegacyAccessLatency()) ) {
            return false;
        }
        if ( !other.getLegacyRetentionPolicy().equals( this.getLegacyRetentionPolicy()) ) {
            return false;
        }

        /**
         * Any non-zero number of occurrences of a URI are considered equivalent.  A necessary
         * condition for equality is that, for each location URI in this object, the other
         * GenericStorageInfo must have at least one instance of the URI.  Having more than one
         * is OK.
         */
        Set<URI> ourLocations = new HashSet<>( locations());
        Set<URI> otherLocations = new HashSet<>( other.locations());

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

        if (!Objects.equals(other.getHsm(), this.getHsm())) {
            return false;
        }

        if (!Objects.equals(other.getCacheClass(), this.getCacheClass())) {
            return false;
        }

        if (!Objects.equals(other._keyHash, this._keyHash)) {
            return false;
        }

        if (other.getLegacySize() != this.getLegacySize() ) {
            return false;
        }
        if (!Objects.equals(other.getStorageClass(), this.getStorageClass())) {
            return false;
        }

        if (other.isStored() != this.isStored() ) {
            return false;
        }
        if (other.isCreatedOnly() != this.isCreatedOnly() ) {
            return false;
        }

        return true;
    }


    /**
     * Allow pre 1.8 storage info to be read. Allows old persistent storage info
     * objects on pools to be read.
     */
    Object readResolve() {

        if(_accessLatency == null ) {
            _accessLatency =  AccessLatency.NEARLINE;
        }

        if(_retentionPolicy == null ) {
            _retentionPolicy = RetentionPolicy.CUSTODIAL;
        }

        if(_locations == null ) {
            _locations = new ArrayList<>();
        }

        return this;

    }

    @Override
    public GenericStorageInfo clone()
    {
        try {
            GenericStorageInfo copy = (GenericStorageInfo) super.clone();
            copy._keyHash = new HashMap<>(_keyHash);
            copy._locations = new ArrayList<>(_locations);
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
                throw new IllegalArgumentException("Invalid format: expected<x:y@z> got <" + storeUnit + '>');
            }
            si = new GenericStorageInfo(unitParts[1], unitParts[0]);
        }

        if (!cacheClass.equals("*")) {
            si.setCacheClass(cacheClass);
        }
        return si;
    }

    @Override
    public void setLegacyAccessLatency(AccessLatency al) {
        _accessLatency = al;
    }

    @Override
    public AccessLatency getLegacyAccessLatency()
    {
        return _accessLatency;
    }

    @Override
    public void setLegacyRetentionPolicy(RetentionPolicy rp) {
        _retentionPolicy = rp;
    }

    @Override
    public RetentionPolicy getLegacyRetentionPolicy()
    {
        return _retentionPolicy;
    }

    @Override
    public long getLegacySize() {
        return _fileSize;
    }

    @Override
    public void setLegacySize(long fileSize) {
        _fileSize = fileSize;
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        if (_keyHash != null) {
            _keyHash = _keyHash.entrySet().stream().collect(toMap(e -> e.getKey().intern(), e -> e.getValue()));
        }
        if (_storageClass != null) {
            _storageClass = _storageClass.intern();
        }
        if (_cacheClass != null) {
            _cacheClass = _cacheClass.intern();
        }
        if (_hsm != null) {
            _hsm = _hsm.intern();
        }
    }
}
