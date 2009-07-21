package diskCacheV111.namespace;

import java.util.Set;
import java.util.List;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.util.Checksum;

public interface NameSpaceProvider
{
    /**
     * set if there is no old value
     */
    public static final int SI_EXCLUSIVE = 0;
    /**
     * replace old value with new one
     */
    public static final int SI_OVERWRITE = 1;
    /**
     * append new value to the old one
     */
    public static final int SI_APPEND = 2;

    /**
     * set file metadata - size, permissions, Owner and group
     * @param pnfsId
     * @param metaData
     * @throws CacheException
     */
    void setFileMetaData(PnfsId pnfsId, FileMetaData metaData) throws CacheException;

    /**
     * get file metadata - size, permissions, Owner and group
     * @param pnfsId
     * @return
     * @throws CacheException
     */
    FileMetaData getFileMetaData(PnfsId pnfsId) throws CacheException;


    /**
     * create file or directory for given path
     * @param path full path of new object
     * @param metaData initial values for object metadata, like owner, group, permissions mode
     * @param isDirectory create a directory if true
     * @return PnfsId of newly created object
     * @throws CacheException
     */
    PnfsId createEntry(String path, FileMetaData metaData, boolean isDirectory) throws CacheException;

    /**
     * remove file or directory associated with given pnfsid
     * @param pnfsId
     * @throws CacheException
     */
    void deleteEntry(PnfsId pnfsId) throws CacheException;

    /**
     * remove file or directory
     * @param path
     * @throws CacheException
     */
    void deleteEntry(String path) throws CacheException;

    void renameEntry(PnfsId pnfsId, String newName) throws CacheException;

    String pnfsidToPath(PnfsId pnfsId) throws CacheException;
    PnfsId pathToPnfsid(String path, boolean followLinks) throws CacheException;

    PnfsId getParentOf(PnfsId pnfsId) throws CacheException;

    String[] getFileAttributeList(PnfsId pnfsId) throws CacheException;
    Object getFileAttribute(PnfsId pnfsId, String attribute) throws CacheException;
    void removeFileAttribute(PnfsId pnfsId, String attribute) throws CacheException;
    void setFileAttribute(PnfsId pnfsId, String attribute, Object data) throws CacheException;

    /**
     * Adds new or replaces existing checksum value for the specific file and checksum type.
     * The type of the checksum is arbitrary integer that client of this interface must chose
     * and use consistently afterwards
     * @param type the type (or algorithm) of the checksum
     * @param value HEX presentation of the digest (checksum)
     * @param pnfsId file
     */
    void addChecksum(PnfsId pnfsId, int type, String value) throws CacheException;

    /**
     * Returns HEX presentation of the checksum value for the specific file and checksum type.
     * Returns null if value has not been set
     * @param type the type (or algorithm) of the checksum
     * @param pnfsId file
     */
    String getChecksum(PnfsId pnfsId, int type) throws CacheException;

    /**
     * Clears checksum value storage for the specific file and checksum type.
     * @param type the type (or algorithm) of the checksum
     * @param pnfsId file
     */
    void removeChecksum(PnfsId pnfsId, int type) throws CacheException;


    int[] listChecksumTypes(PnfsId pnfsId) throws CacheException;

    Set<Checksum> getChecksums(PnfsId pnfsId) throws CacheException;

    StorageInfo getStorageInfo(PnfsId pnfsId) throws CacheException;
    void setStorageInfo(PnfsId pnfsId, StorageInfo storageInfo, int mode) throws CacheException;


    /**
     * add a cache location for a file
     * @param pnfsId of the file
     * @param cacheLocation the new location
     * @throws CacheException
     */
    void addCacheLocation(PnfsId pnfsId, String cacheLocation) throws CacheException;

    /**
     * get all cache location of the file
     * @param pnfsId of the file
     * @return list containing locations or empty list, if locations are unknown
     * @throws CacheException
     */
    List<String> getCacheLocation(PnfsId pnfsId) throws CacheException;

    /**
     * clear cache locations
     * @param pnfsId of the file
     * @param cacheLocation, "*" forces to remove all known locations
     * @param removeIfLast remove entry from namespace if last known location is removed
     * @throws CacheException
     */
    void clearCacheLocation(PnfsId pnfsId, String cacheLocation, boolean removeIfLast) throws CacheException;
}
