package diskCacheV111.vehicles;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

/**
  *  Implementations of this thing travel from the particular
  *  Door to the corresponding mover. The relevant information
  *  is extracted out of Pnfs with the help of the
  *  responsible implementation of <strong>StorageInfoExtractor</strong>
  *  according to hsmType.
  *
  *
  *  to represent location of the file with in HSM hierarchical syntax is used:
  *
  *  <strong>[scheme:][//authority][path][?query][#fragment]</strong>
  *  where:
  * 	scheme    : hsm type
  * 	authority : instance id
  * 	path+query: opaque to dCache HSM specific data
  *
  *  example:
  * 	osm://desy-main/?store=h1&bfid=1234
  *		osm://desy-copy/?store=h1_d&bfid=5678
  *
  *
  */
public interface StorageInfo
    extends Serializable, Cloneable
{

    /**
     * Classic dCache default all files go to tape and can be removed
     * from the pool afterwards.
     */
    AccessLatency DEFAULT_ACCESS_LATENCY = AccessLatency.NEARLINE;
    RetentionPolicy DEFAULT_RETENTION_POLICY = RetentionPolicy.CUSTODIAL;

    long serialVersionUID = 1623022255585848311L;
    /**
      *   The storage class is a unique string, identifying
      *   this particular storage entity in a HSM independent
      *   way ( what else, its a string).
      *   The storage class reflects a HSM specific organizational
      *   unit. ( storage group for OSM, and file family for enstore).
      *   The storage class determines the grouping behavior
      *   concerning <strong>deferredWrites</strong> and
      *   <strong>poolSelection</strong>.
      *
      */
    @Deprecated
    String getStorageClass() ;
    @Deprecated
    void setStorageClass(String newStorageClass);

    @Deprecated
    /**
     * use addLocation(URI newLocation);
     */ void   setBitfileId(String bitfileId) ;
    @Deprecated
    /**
     * use List<URI> locations();
     */ String getBitfileId() ;

    /**
     *
     * @return list of know locations
     * @since 1.8
     */
    List<URI> locations();

    /**
     * add a new location for the file
     * @param newLocation
     * @since 1.8
     */
    void addLocation(URI newLocation);

    /**
     *
     * @return true if new location is added and
     * have to be stored by PnfsManager
     * @since 1.8
     */
    boolean isSetAddLocation();
    void isSetAddLocation(boolean isSet);


    /**
      * The 'cacheClass' can be used as alternative to chose the
      * appropriate 'pool'. Pnfs may provide the information
      * in the 'cacheClass' tag. May return 'null'  if not
      * precified.
      */
    @Deprecated
    String getCacheClass() ;
    @Deprecated
    void setCacheClass(String newCacheClass);
    /**
      * Returns the name of the HSM or the HSMInstance.
      */
    @Deprecated
    String getHsm() ;
    @Deprecated
    void setHsm(String newHsm);
    /**
      * Determines whether the file exists somewhere (cache or HSM)
      * or not. Currently isCreatedOnly returns true is the
      * size of the level-0 file is not zero.
      */
    boolean isCreatedOnly() ;
    void setIsNew(boolean isNew);
    /**
      *
      * @return true if locations list is not empty or ( legacy case )
      * if value was explicit set by setIsStored(true)
      */
    boolean isStored() ;

	/**
	 * @Deprecated the result will generated depending on content of locations
	 */
    @Deprecated
    void setIsStored(boolean isStored);
    /**
      *  The storage info may contain HSM specific key value pairs.
      *  Nobody should rely on the existence of a particular key.
      *  A 'null' is returned if no related key-value pair could be
      *  found.
      */
    String  getKey(String key) ;

    /**
     * add/set new value for specified key. If value is null,
     * corresponding entry is removed.
     * @param key
     * @param value
     * @return The previous value associated with key
     */
    String setKey(String key, String value) ;
    /**
      * Returns a COPY of the internal key,value map.
      */
    Map<String, String>     getMap() ;


    /*
     * specify which fields have to be updated
     */

    boolean isSetHsm();
    void isSetHsm(boolean isSet);

    boolean isSetStorageClass();
    void isSetStorageClass(boolean isSet) ;

    boolean isSetBitFileId();
    void isSetBitFileId(boolean isSet);

    StorageInfo clone();

    @Override
    String toString();

    @Deprecated
    long getLegacySize();

    @Deprecated
    void setLegacySize(long fileSize);

    @Deprecated
    void setLegacyAccessLatency(AccessLatency al);

    @Deprecated
    AccessLatency getLegacyAccessLatency();

    @Deprecated
    void setLegacyRetentionPolicy(RetentionPolicy rp);

    @Deprecated
    RetentionPolicy getLegacyRetentionPolicy();
}
